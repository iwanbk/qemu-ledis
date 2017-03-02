#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <assert.h>

#include "list.h"
#include "hiredis/hiredis.h"
#include "uthash.h"
#include "ldb_md5.h"
#include "ldb_meta.h"

#define LOG(msg, ...) do { \
    fprintf(stderr, "%s:%s():L%d: " msg "\n", \
    __FILE__, __FUNCTION__, __LINE__, ## __VA_ARGS__); \
    } while (0)


#define MAX_LDB_META_CACHE_ENTRY 150
#define LDB_META_THREAD_SLEEP_MICROSEC ( 50 * 1000) /* 50 milliseconds */

/** temporary variable to enable-disable threaded ldb_meta **/
#define LDB_META_THREADED_ON 0 /* flag to enable/disable threaded metadata */
static redisContext *rds_ctx;
/** end of temporary variable to enable-disable threaded ldb_meta **/


// meta cache entry
typedef struct ldb_meta_cache_entry_t {
    ssize_t key;
    page_md5_t value;
    UT_hash_handle hh;
} ldb_meta_cache_entry_t;

// ldb meta pthread data
typedef struct ldb_meta_pthread_data_t {
    redisContext *ctx;
    int vol_id;
}ldb_meta_pthread_data_t;

// ldb metadata
typedef struct ldb_meta_data_t {
    ssize_t page;           // page number
    page_md5_t page_md5;    // metadata of this page
    struct list_head list;  // needed for the list
}ldb_meta_data_t;

// ldb meta data queue
typedef struct ldb_meta_qeueu_t {
    ssize_t size;
    ldb_meta_data_t queue;
    pthread_mutex_t mutex;
}ldb_meta_queue_t;

// ldb meta cache object
static ldb_meta_cache_entry_t *ldb_meta_cache = NULL;

// ldb meta pthread object
static pthread_t ldb_meta_pthread;

// ldb meta queue object
static ldb_meta_queue_t ldb_meta_queue;

static MD5_t empty_md5;

static void _free_ldb_meta_data(ldb_meta_data_t *md)
{
}

/**
 * set meta of a page, synchronous version.
 * page = page number
 * page_md5 = meta for that page.
 */
static int _ldb_meta_set_to_server(redisContext *ctx, ssize_t page, page_md5_t page_md5)
{
    int err = 0;
    char dataCmd[100];
    sprintf(dataCmd, "%lu", page);

    //LOG("write meta for page=%ld", page);

	redisReply* reply = redisCommand(ctx, "SET %b %b", dataCmd, strlen(dataCmd), page_md5, PAGE_MD5_LEN);

	if (reply->type == REDIS_REPLY_ERROR)
	{
		err = -EIO;
	}
	else if (reply->type == REDIS_REPLY_NIL)
	{
		err = -ENOENT;
	}
    freeReplyObject(reply);
    return err;
}

// enqueue page meta data
static int _ldb_meta_enqueue(ssize_t page, page_md5_t pm) {
    ldb_meta_data_t *md = (ldb_meta_data_t *) malloc (sizeof (ldb_meta_data_t));
    if (!md) {
        return -ENOMEM;
    }
    INIT_LIST_HEAD(&md->list);
    md->page = page;
    memcpy(md->page_md5, pm, PAGE_MD5_LEN);

    pthread_mutex_lock(&ldb_meta_queue.mutex);

    list_add(&(md->list), &(ldb_meta_queue.queue.list));
    ldb_meta_queue.size += 1;

    pthread_mutex_unlock(&ldb_meta_queue.mutex);

    return 0;
}

static ldb_meta_data_t* _ldb_meta_dequeue(void)
{
    struct list_head *pos, *q;
    ldb_meta_data_t *tmp;

    pthread_mutex_lock(&ldb_meta_queue.mutex);
    if (ldb_meta_queue.size == 0) {
        pthread_mutex_unlock(&ldb_meta_queue.mutex);
        return NULL;
    }

    list_for_each_safe(pos, q, &ldb_meta_queue.queue.list) {
        tmp = list_entry(pos, ldb_meta_data_t, list);
        list_del(pos);
        ldb_meta_queue.size -= 1;
        break;
    }
    pthread_mutex_unlock(&ldb_meta_queue.mutex);
    return tmp;

}
/**
 * update metadata:
 * - write it to cache (immediately)
 * - put in meta queue. It will be written by ldb_meta thread
 */
int ldb_meta_update(ssize_t key, page_md5_t pm)
{
    ldb_meta_cache_entry_t *entry;
    HASH_FIND_INT(ldb_meta_cache, &key, entry);

    if (!entry)
    {
        LOG("ERROR : ldb_meta_cache_update failed to get entry for page:%ld", key);
        return -EIO;
    }
    memcpy(entry->value, pm, PAGE_MD5_LEN);

    if (LDB_META_THREADED_ON == 1) {
        return _ldb_meta_enqueue(key, pm);
    } else {
        return _ldb_meta_set_to_server(rds_ctx, key, pm);
    }
}

/**
 * get metadata of a page, synchronous version.
 * page = page number.
 * page_md5 = md5 for that page
 */
static int _ldb_meta_get_from_server(redisContext *ctx, ssize_t page, page_md5_t page_md5)
{
    char query[100];
    int err = 0;
    bzero(page_md5, PAGE_MD5_LEN);

    sprintf(query, "GET %lu", page);

    redisReply *reply = redisCommand(ctx, query);
    if (reply->type == REDIS_REPLY_ERROR)
    {
        err = -EIO;
    }
	else if ((reply->type == REDIS_REPLY_NIL) || (reply->type == REDIS_REPLY_STATUS))
	{
		//TODO : s->meta.missingReads ++;
		err = -ENOENT;
	}
	else
	{
		//TODO : s->meta.reads ++;
		//assert (reply->len == PAGE_MD5_LEN);
		//assert (reply->type == REDIS_REPLY_STRING);

		memcpy(page_md5, reply->str, reply->len);
	}
    freeReplyObject(reply);
    return err;
}

/**
 * ldb_meta_cache_add
 * add a cache entry
 */
static int _ldb_meta_cache_add(ssize_t key, page_md5_t pm)
{
    ldb_meta_cache_entry_t *entry, *tmpEntry;

    // initialize cache object
    entry = malloc(sizeof(ldb_meta_cache_entry_t));
    entry->key = key;
    memcpy(entry->value, pm, PAGE_MD5_LEN);

    // add to cache
    HASH_ADD_INT(ldb_meta_cache, key, entry);

    // check if cache full
    if (HASH_COUNT(ldb_meta_cache) > MAX_LDB_META_CACHE_ENTRY)
    {
        // delete oldest item
        HASH_ITER(hh, ldb_meta_cache, entry, tmpEntry) {
            //LOG("remove cache for page :%ld", entry->key);
            HASH_DELETE(hh, ldb_meta_cache, entry);
            free(entry);
            break;
        }
    }
    return 0;
}


/**
 * get metadata.
 * it will try to find the data from cache first.
 * if not exist in cache, it will find it from ledisdb.
 */
int ldb_meta_get(redisContext *ctx, ssize_t key, page_md5_t pm)
{
    int ret;
    ldb_meta_cache_entry_t *entry;
    HASH_FIND_INT(ldb_meta_cache, &key, entry);

    if (!entry) // get from database if not exist in cache
    {
        // get from db
        ret = _ldb_meta_get_from_server(ctx, key, pm);
        if (ret != 0 && ret != -ENOENT)
        {
            return ret;
        }
        // add to cache
        _ldb_meta_cache_add(key, pm);
    }
    else
    {
        // move to front
        HASH_DEL(ldb_meta_cache, entry);
        HASH_ADD_INT(ldb_meta_cache, key, entry);
        memcpy(pm, entry->value, PAGE_MD5_LEN);
    }

    return 0;
}


/**
 * dequeue element from queue and send it to mdserver.
 */
static void *ldb_meta_pthread_func(void *data)
{
    ldb_meta_pthread_data_t *pd = data;
    int ret;

    LOG("Starting ldb_meta thread. vol_id = %d", pd->vol_id);
    while(1) {
        usleep(LDB_META_THREAD_SLEEP_MICROSEC);
        // dequeue metadata
        struct ldb_meta_data_t *md = _ldb_meta_dequeue();
        if (!md) {
            continue;
        }

        // write metadata
        ret = _ldb_meta_set_to_server(pd->ctx, md->page, md->page_md5);
        if (ret != 0) {
            LOG("failed to set metadata to server:%d", ret);
        }
        _free_ldb_meta_data(md);
    }
    return NULL;
}

/**
 * initialize ldb metadata module:
 * - init ldb meta queue
 * - init ldb meta thread
 */
int ldb_meta_init(redisContext *ctx, int vol_id)
{
    int ret;

    rds_ctx = ctx;

    if (LDB_META_THREADED_ON == 0) {
        LOG("initialized ldb_meta module with threaded = OFF");
        return 0;
    }

    // initialize metaqueue
    INIT_LIST_HEAD(&ldb_meta_queue.queue.list);
    ret = pthread_mutex_init(&ldb_meta_queue.mutex, NULL);
    if (ret != 0) {
        LOG("failed to initialize ldb meta queue mutex:%d", ret);
        return ret;
    }

    // create pthread
    ldb_meta_pthread_data_t *pd = (ldb_meta_pthread_data_t *) malloc (sizeof(ldb_meta_pthread_data_t));
    pd->ctx = ctx;
    pd->vol_id = vol_id;

    ret = pthread_create(&ldb_meta_pthread, NULL, ldb_meta_pthread_func, (void *)pd);
    if (ret != 0) {
        LOG("ldb_meta pthread creation failed:%d",ret);
        return ret;
    }
    return 0;
}

/**
 * Get block meta from aggregated page metadata
 */
int ldb_meta_pages_get_meta_block(ssize_t off, ssize_t page, page_md5_t page_md5, MD5_t md5)
{
    // check if this offset belong to this page
    assert(OFF_TO_PAGE(off) == page);

    int relBlock = BLOCK_NUMBER(off) % 256; // relative block number of this block in this page
    memcpy(md5, page_md5 + (MD5_LEN * relBlock), MD5_LEN);

    // check if empty
    if (memcmp(empty_md5, md5, MD5_LEN) == 0)
    {
        return -ENOENT;
    }
    return 0;
}

/**
 * set block meta to aggregated page metadata
 */
void ldb_meta_pages_set_meta_block(ssize_t off, ssize_t page, page_md5_t page_md5, MD5_t md5)
{
    // check if this offset belong to this page
    assert(OFF_TO_PAGE(off) == page);

    int relBlock = BLOCK_NUMBER(off) % 256; // relative block number of this block in this page
    memcpy(page_md5 + (MD5_LEN * relBlock), md5, MD5_LEN);
}
