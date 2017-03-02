/*
 * QEMU Block driver for  LedisDB
 *
 * Copyright (C) 2014
 *     Author: Sandeep Joshi, Iwan Budi Kusnanto, Arne Tossens
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <errno.h>
#include "qemu/uri.h"
#include "block/block_int.h"
#include "qemu/module.h"
#include "qemu/sockets.h"
#include "qapi/qmp/qdict.h"
#include "qapi/qmp/qjson.h"
#include "qapi/qmp/qint.h"
#include "qapi/qmp/qstring.h"

#include "hiredis/hiredis.h"
#include "hiredis/async.h"

#include "NonAlignedCopy.h"
#include "qemu/crc32c.h"

#include "ldb_int.h"
#include "ldb_crypto.h"
#include "ldb_tlog.h"
#include "ldb_md5.h"
#include "ldb_meta.h"
#include "ldb_sc.h"
#include "uthash.h"

#include <openssl/md5.h>
#include <sys/types.h>
#include <unistd.h>

// Empirically set batchsize to 8
// Ensure conn_read_buffer_size in config is large enough
// See https://github.com/siddontang/ledisdb/issues/135
#define WRITE_BATCHSIZE 8

// Empirically set small-mode up to 32 sectors
#define MAX_SMALL_MODE 32L

#define METAHOST "%d.md.grid1.com"
#define METAPORT 6380

#define DATA_DOMAIN "%.2x.storage.grid1.com"
#define DATAPORT 6380

#define TLOG_DOMAIN "%d.tlog.grid1.com"
#define TLOG_PORT 6380

#define TRACE(msg, ...) do { \
    LOG(msg, ## __VA_ARGS__); \
    } while (0)

#define LOG(msg, ...) do { \
    fprintf(stderr, "%s:%s():L%d: " msg "\n", \
    __FILE__, __FUNCTION__, __LINE__, ## __VA_ARGS__); \
    } while (0)

#define PAGE_MD5_LEN (16 * 256)
typedef char page_md5_t[MD5_LEN * 256];

#define SECTOR_SIZE (512)
#define LDB_BLKSIZE (4096)
#define LDB_DATA_NUMBER_GET(md5) ((uint8_t)(md5[0]))

/**
 * ledis meta data cache entry using uthash.
 * based on uthash LRU cache example https://github.com/troydhanson/uthash/blob/master/tests/test65.c
 */

/* volume ID of this volume driver */
static int VOL_ID;

/* snapshot ID for this run - get it from qemu parameter? */
static int SNAPSHOT_ID = 0;

/* volume size of this volume driver, in bytes */
static long int VOL_SIZE;

static MD5_t empty_md5;

static void ldb_parse_filename(const char *filename, QDict *dict,
                               Error **errp)
{
    char *file;
    const char *vol_id_size;
    //char **strarr;

    file = g_strdup(filename);

    // extract the host_spec - fail if it's not ldb:...
    if (!strstart(file, "ldb://", &vol_id_size)) {
        error_setg(errp, "File name string for LDB must start with 'ldb:'");
        goto out;
    }

    if (!*vol_id_size) {
        goto out;
    }
    qdict_put(dict, "vol_id_size", qstring_from_str(vol_id_size));
out:
    g_free(file);
}

static int ldb_establish_connection(BlockDriverState *bs, Error** errp)
{
	int i;
	struct errstruct es;
    LDBState *s = bs->opaque;

	LOG("connecting to ledis data");
	for (i = 0; i < 256 ; i++) {
		s->data[i].context = sharedconn_addref(&es, s->data[i].host, s->data[i].port, DATA_TYPE);

		if (s->data[i].context == NULL || es.err)
		{
			if (s->data[i].context)
			{
				error_setg_errno(errp, -ENOTCONN, "Connection error to data server: %s", es.errstr);
				sharedconn_release(s->data[i].context, &es);
			}
			else
			{
				error_setg_errno(errp, -ENOTCONN, "Connection error: can't allocate redis context\n");
			}
			return -ENOTCONN;
		}
#if (LOGME)
        s->data[i].context->fp = s->fp;
#endif
	}

    LOG("connecting to ledis md");
    s->meta.context = redisConnect(s->meta.host, s->meta.port);

    if (s->meta.context == NULL || s->meta.context->err)
    {
        if (s->meta.context)
        {
            error_setg_errno(errp, -ENOTCONN, "Connection error to meta server: %s", s->meta.context->errstr);
            redisFree(s->meta.context);
        }
        else
        {
            error_setg_errno(errp, -ENOTCONN, "Connection error: can't allocate redis context\n");
        }

		for (i = 0; i < 256; i++) {
			sharedconn_release(s->data[i].context, &es);
		}
        return -ENOTCONN;
    }

    LOG("connecting to tlog server");
    s->tlog.context = tlog_open(VOL_ID, SNAPSHOT_ID);

    if (s->tlog.context == NULL || s->tlog.context->err)
    {
        if (s->tlog.context)
        {
            error_setg_errno(errp, -ENOTCONN, "Connection error to tlog: %s", s->tlog.context->errstr);
            tlog_close(s->tlog.context);
        }
        else
        {
            error_setg_errno(errp, -ENOTCONN, "Connection error: can't open tlog context\n");
        }

		for (i = 0; i < 256; i++) {
			sharedconn_release(s->data[i].context, &es);
		}
        return -ENOTCONN;
    }

    // Send Ping to LDB and get Pong back
    //result = ldb_client_session_init(&s->client, bs, sock, export);
    //g_free(export);


    return 0;
}

static QemuOptsList runtime_opts = {
    .name = "ldb",
    .head = QTAILQ_HEAD_INITIALIZER(runtime_opts.head),
    .desc = {
        {
            .name = "filename",
            .type = QEMU_OPT_STRING,
            .help = "LDB image name",
        },
        { /* end of list */ }
    },
};

static char *ldb_config_item(const char *args, const char *item) {
    char *temp, *end;
    
    // key not found
    if(!(temp = strstr(args, item)))
        return NULL;
    
    temp += strlen(item);
    
    // mal formated
    if(*temp != '=')
        return NULL;
    
    if(!(end = strchr(temp, ':')))
        return strdup(temp + 1);
    
    return strndup(temp + 1, end - temp - 1);
}

static int ldb_config(LDBState* s, QDict* dict, Error** errp)
{
	int i;
	int ret;
    Error* local_err = NULL;
    char *value;

    QemuOpts* opts = qemu_opts_create(&runtime_opts, NULL, 0, &error_abort);
    qemu_opts_absorb_qdict(opts, dict, &local_err);

    // parse option
    const char *vol_id_size = qdict_get_str(dict,"vol_id_size");
    
    // volume id
    if(!(value = ldb_config_item(vol_id_size, "volid"))) {
        LOG("missing 'volid' option");
        return -1;
    }
    
    VOL_ID = atoi(value);
    free(value);
    
    // volume size
    if(!(value = ldb_config_item(vol_id_size, "volsize"))) {
        LOG("missing 'volsize' option");
        return -1;
    }
    
    VOL_SIZE = atol(value);
    free(value);
    
    // round if not blocksize aligned
    VOL_SIZE = ((VOL_SIZE + (LDB_BLKSIZE - 1)) / LDB_BLKSIZE) * LDB_BLKSIZE;
    
    // tlog
    if(!(value = ldb_config_item(vol_id_size, "tlog"))) {
        LOG("missing 'tlog' option");
        return -1;
    }
    
    TLOG_FOLDER_ROOT = value;

    LOG("starting vol driver id: %d size: %ld bytes", VOL_ID, VOL_SIZE);
    LOG("tlog folder root: %s", TLOG_FOLDER_ROOT);

    // assign ledisdb data address
    for (i = 0; i < 256; i++) {
		ret = asprintf(&s->data[i].host, DATA_DOMAIN, i);
		if (ret < 0) {
			return ret;
		}
		s->data[i].port = DATAPORT;
	}

    // assign ledisdb meta address
    asprintf(&s->meta.host, METAHOST, VOL_ID);
    s->meta.port = METAPORT;

    LOG("metahost = %s", s->meta.host);

    qemu_opts_del(opts);
    qdict_del(dict, "vol_id_size");

#if (LOGME)
    s->fp = fopen("/tmp/ldb.log", "w");
#endif
    return 0;
}

static int ldb_open(BlockDriverState *bs, QDict *dict, int flags,
                    Error **errp)
{
    int i;
    LDBState *s = bs->opaque;

    bzero(empty_md5, MD5_LEN);
    // init state data
    for (i = 0; i < 256; i++) {
        s->data[i].reads = s->data[i].writes = s->data[i].missingReads = 0;
    }
    s->meta.reads = s->meta.writes = s->meta.missingReads = 0;
    s->readModifyWrites = 0;

    ldb_config(s, dict, errp);


    int ret = ldb_establish_connection(bs, errp);
    if (ret != 0) {
        return ret;
    }

    LOG("opened connection with ret=%d\n", ret);

    ret = ldb_meta_init(s->meta.context, VOL_ID);
    if (ret != 0) {
        LOG("ldb meta initialisation failed:%d", ret);
        return ret;
    }
    ret = tlog_init(s->tlog.context);
    if (ret != 0) {
        LOG("tlog initialisation failed with ret=%d",ret);
    }
    return ret;
}

// ============================

/**
 * Flush pipelined redis command - read version
 */
static int RedisReadPipelineFlush(struct errstruct *esp, LDBState* s, uint8_t ldbId) {
    return sharedconn_bufferwrite(s->data[ldbId].context, esp);
}

/**
 * Flush pipelined redis command - write version
 */
static int RedisWritePipelineFlush(struct errstruct *esp, int smallMode, LDBState* s, uint8_t ldbId) {
    if (smallMode) {
        return sharedconn_bufferwrite(s->data[ldbId].context, esp);
    }
    else {
        return sharedconn_flush_batch(s->data[ldbId].context, esp, WRITE_BATCHSIZE);
    }
}

/**
 * pipelined write/set data to ledisdb data
 */
static int DataSetCmdPipeline(struct errstruct *esp, int smallMode, LDBState* s, uint8_t ldbId, MD5_t md5, const char* buffer)
{
    int localResult;

    if (smallMode) {
        localResult = sharedconn_appendcommand(s->data[ldbId].context, esp, "SET %b %b", md5, MD5_LEN, (void *) buffer, LDB_BLKSIZE);    }
    else {
        localResult = sharedconn_append_keyvalue(s->data[ldbId].context, esp, md5, buffer, WRITE_BATCHSIZE);
    }

    if (localResult != REDIS_OK)
    {
        error_report("SET command issue error: %d\n", localResult);
        return -EIO;
    }
    return 0;
}

/**
 * check reply of previous pipelined write/set data
 */
static int DataCheckReplyPipeline(struct errstruct *esp, int smallMode, LDBState* s, uint8_t ldbId)
{
    redisReply* reply = NULL;

    if (smallMode) {
        sharedconn_getreply(s->data[ldbId].context, esp, (void**) &reply);
    }
    else {
        // Flush everything in the batch cache
        sharedconn_flush_batch(s->data[ldbId].context, esp, 0);
        // Get reply from queue
        sharedconn_popreply(s->data[ldbId].context, esp, (void**) &reply);
    }

    if (reply->type == REDIS_REPLY_ERROR)
    {
        error_report("SET error: %s", reply->str);
        sharedconn_freereply(s->data[ldbId].context, esp, reply);
        return -EIO;
    }
    sharedconn_freereply(s->data[ldbId].context, esp, reply);
	s->data[ldbId].writes ++;
    return 0;
}

/**
 * get data from ledisdb-data, synchronous version
 */
static int DataGetCmd(struct errstruct *esp, LDBState* s, MD5_t md5, char* buffer)
{
    int err = 0;
	int ldbId = LDB_DATA_NUMBER_GET(md5);

    redisReply *reply = sharedconn_command(s->data[ldbId].context, esp, "GET %b", md5, MD5_LEN);

	if (reply->type == REDIS_REPLY_ERROR)
	{
        error_report("GET error: %s %d %s\n", reply->str, esp->err, esp->errstr);
		err = -EIO;
	}
	else if ((reply->type == REDIS_REPLY_NIL) || (reply->type == REDIS_REPLY_STATUS))
	{
		err = -ENOENT;
	}
	else
	{
        if (reply->len != LDB_BLKSIZE || reply->type != REDIS_REPLY_STRING)
        /**
         * i still don't understand how it could happen.
         * because we only set string both in metadata and data.
         * I only log it and not take it as error because when
         * i got this condition, the file still valid (verified using 'cmp' command).
         */
        {
            LOG("WARNING_TO_CHECK : reply->len = %d",reply->len);
            LOG("WARNING_TO_CHECK redis reply type = %d", reply->type);
            //assert (reply->len == LDB_BLKSIZE);
            //assert (reply->type == REDIS_REPLY_STRING);
        }
		s->data[ldbId].reads ++;

		memcpy(buffer, reply->str, reply->len);
	}
    freeReplyObject(reply);
    return err;
}

/**
 * get data from ledisdb data, pipelined version
 */
static int DataGetCmdPipeline(struct errstruct *esp, LDBState* s, MD5_t md5, uint8_t ldbId)
{
    int localResult  = sharedconn_appendcommand(s->data[ldbId].context, esp, "GET %b", md5, MD5_LEN);

    if (localResult != REDIS_OK)
    {
        error_report("GET command issue error: %d\n", localResult);
        return -EIO;
    }
    return 0;
}

/**
 * check reply of previous pipelined get data
 */
static int DataGetReplyPipeline(struct errstruct *esp, LDBState* s, uint8_t ldbId, char* buffer, ssize_t startOffset, ssize_t bufferSize)
{
    int err = 0;
    redisReply* reply = NULL;
    sharedconn_getreply(s->data[ldbId].context, esp, (void**) &reply);

    if (reply->type == REDIS_REPLY_ERROR)
    {
        error_report("GET error: %s %d %s\n", reply->str, esp->err, esp->errstr);
        err = -EIO;
    }
    else
    {
        if (reply->type == REDIS_REPLY_NIL)
        {
            // block was never written - no md5 exists for offset
			s->data[ldbId].missingReads ++;
            err = -ENOENT;
        }
        else
        {
			s->data[ldbId].reads ++;
            assert(reply->len == LDB_BLKSIZE);
            assert(reply->len >= bufferSize);
            assert(startOffset < LDB_BLKSIZE);
            assert (reply->type == REDIS_REPLY_STRING);
            memcpy(buffer, reply->str + startOffset, bufferSize);
        }
    }
    sharedconn_freereply(s->data[ldbId].context, esp, reply);
    return err;
}




// ============================

/*
 *  MetaData : key=offset, value=md5
 *  Data     : key=md5, value=buffer
 */
#if (LOGME)
static int _ldb_read(BlockDriverState *bs, int64_t sector_num, uint8_t *buf, int nb_sectors);
static int  ldb_read(BlockDriverState *bs, int64_t sector_num, uint8_t *buf, int nb_sectors)
{
	int retval;
	struct timeval tv;
	gettimeofday(&tv, NULL);
    LDBState *s = bs->opaque;
    fprintf(s->fp, "rb %ld,%ld %ld %d\n", tv.tv_sec, tv.tv_usec, sector_num, nb_sectors);
	retval = _ldb_read(bs, sector_num, buf, nb_sectors);
	gettimeofday(&tv, NULL);
    fprintf(s->fp, "re %ld,%ld %d\n", tv.tv_sec, tv.tv_usec, retval);
	return retval;
}
static int _ldb_read(BlockDriverState *bs, int64_t sector_num, uint8_t *buf, int nb_sectors)
#else
static int  ldb_read(BlockDriverState *bs, int64_t sector_num, uint8_t *buf, int nb_sectors)
#endif
{
    LDBState *s = bs->opaque;
    struct errstruct es;
    int result = 0;

    /*
        md5 = control->get(block)
        if (md5)
            buffer = data->get(md5)
            memcpy(newbuf, buffer, 4k)
        else
            bzero(newbuf, 4k)
    */

    const size_t blocksToReadSz = ((nb_sectors/8) + 2);
    char* blocksToRead = (char*)malloc(blocksToReadSz);
	uint8_t* ldbIdsToRead = (uint8_t*)malloc(blocksToReadSz);
    bzero(blocksToRead, blocksToReadSz);
    bzero(ldbIdsToRead, blocksToReadSz);

    NonAlignedCopy a;

    int localResult;
    NonAlignedCopyInit(&a, sector_num * SECTOR_SIZE, nb_sectors * SECTOR_SIZE, LDB_BLKSIZE);
    long curPage = 0; // current page this block
    long prevPage = -1;
    page_md5_t page_md5;

    int blocksIndex = 0;

	// send get  data command to ledisdb data
    while (NonAlignedCopyIsValid(&a))
    {
        ssize_t retOff = 0;
        ssize_t retSz = 0;
        NonAlignedCopyNext(&a, &retOff, &retSz);


        // get meta
        MD5_t md5;
        curPage = OFF_TO_PAGE(retOff);

        if (prevPage != curPage)
        {
            //localResult = MetaPagesGetCmd(s, curPage, page_md5);
            localResult = ldb_meta_get(s->meta.context, curPage, page_md5);
            if (localResult != 0 && localResult != -ENOENT)
            {
                result = localResult;
                break;
            }
        }

        localResult = ldb_meta_pages_get_meta_block(retOff, curPage, page_md5, md5);


        // pipeline the get data commands
        if (localResult == 0)
        {
            // fetch the block from data server
            blocksToRead[blocksIndex] = '1';
			ldbIdsToRead[blocksIndex] = LDB_DATA_NUMBER_GET(md5);

            localResult = DataGetCmdPipeline(&es, s, md5, ldbIdsToRead[blocksIndex]);
            if (localResult != 0) {
                LOG("ldb_read() DataGetCmdPipeline failed.result=%d", localResult);
                result = -EIO;
                break;
            }

            localResult = RedisReadPipelineFlush(&es, s, ldbIdsToRead[blocksIndex]);
            if (localResult != 0) {
                LOG("ldb_read() DataGetCmdPipeline redis flush failed.result=%d", localResult);
                result = -EIO;
                break;
            }
            //uint32_t crc = crc32c(0xffffffff, (uint8_t*)reply->str, reply->len);
            //LOG("read at blk=%lu reqsiz=%ld actual=%d checksum=%u\n", BLOCK_NUMBER(retOff), retSz, reply->len, crc);
        }
		else if (localResult == -ENOENT)
        {
        }
    	else
	    {
            result = -EIO;
            break;
        }
        prevPage = curPage;
        blocksIndex ++;
    }

    if (result != 0)
    {
        return result;
    }

    NonAlignedCopyInit(&a, sector_num * SECTOR_SIZE, nb_sectors * SECTOR_SIZE, LDB_BLKSIZE);

    blocksIndex = 0;

	// get reply
    while (NonAlignedCopyIsValid(&a))
    {
        ssize_t retOff = 0;
        ssize_t retSz = 0;
        NonAlignedCopyNext(&a, &retOff, &retSz);

        ssize_t relativeOff = retOff - (sector_num * SECTOR_SIZE);
        assert(relativeOff < nb_sectors * SECTOR_SIZE);
        uint8_t* curBuf = buf + relativeOff;

		if (blocksToRead[blocksIndex] != '1')
        {
          	bzero(curBuf, retSz);
        }
        else
        {
            ssize_t startOffset = retOff % LDB_BLKSIZE; // if retOff is not 4k-aligned, memcpy has to be done in middle of buffer

            int localResult = DataGetReplyPipeline(&es, s, ldbIdsToRead[blocksIndex], (char*) curBuf, startOffset, retSz);

            //uint32_t crc = crc32c(0xffffffff, (uint8_t*)reply->str, reply->len);
            //LOG("read at blk=%lu reqsiz=%ld actual=%d checksum=%u\n", BLOCK_NUMBER(retOff), retSz, reply->len, crc);

            if (localResult != 0)
            {
                LOG("ldb_read() DataGetReplyPipeline failed.result=%d", localResult);
                result = -EIO;
                break;
            }
            //freeReplyObject(reply);
        }
        blocksIndex ++;
    }

    free(blocksToRead);
	free(ldbIdsToRead);

    return 0;
}

void computeMD5(const char* buffer, MD5_t md5);

void computeMD5(const char* buffer, MD5_t md5)
{
    /*
    struct md5_ctx ctx;
    digest_init(&md5_algorithm, &ctx);

    digest_update(&md5_algorithm, &ctx, buffer, LDB_BLKSIZE);

    digest_final(&md5_algorithm, &ctx, md5);
    */
    
    /* openssl implementation */
    MD5((unsigned char *) buffer, LDB_BLKSIZE, (unsigned char *) md5);
}

#define PRE_ALLOC_SECTORS 1024
#define PRE_ALLOC_BLOCKS ((PRE_ALLOC_SECTORS / 8) + 2)
static uint8_t preAllocLdbIdsToWrite[PRE_ALLOC_BLOCKS];
static MD5_t preAllocMD5Array[PRE_ALLOC_BLOCKS];

#if (LOGME)
static int _ldb_write(BlockDriverState *bs, int64_t sector_num, const uint8_t *buf, int nb_sectors);
static int  ldb_write(BlockDriverState *bs, int64_t sector_num, const uint8_t *buf, int nb_sectors)
{
	int retval;
	struct timeval tv;
	gettimeofday(&tv, NULL);
    LDBState *s = bs->opaque;
    fprintf(s->fp, "wb %ld,%ld %ld %d\n", tv.tv_sec, tv.tv_usec, sector_num, nb_sectors);
	retval = _ldb_write(bs, sector_num, buf, nb_sectors);
	gettimeofday(&tv, NULL);
    fprintf(s->fp, "we %ld,%ld %d\n", tv.tv_sec, tv.tv_usec, retval);
	return retval;
}
static int _ldb_write(BlockDriverState *bs, int64_t sector_num, const uint8_t *buf, int nb_sectors)
#else
static int  ldb_write(BlockDriverState *bs, int64_t sector_num, const uint8_t *buf, int nb_sectors)
#endif
{
    LDBState *s = bs->opaque;
    struct errstruct es;
    int result = 0;
    const size_t blocksToWriteSz = ((nb_sectors/8) + 2);
    int smallMode = nb_sectors <= MAX_SMALL_MODE;
	uint8_t* ldbIdsToWrite = blocksToWriteSz <= PRE_ALLOC_BLOCKS ? preAllocLdbIdsToWrite : (uint8_t*) malloc(sizeof(uint8_t) * blocksToWriteSz);
	MD5_t *md5Array = blocksToWriteSz <= PRE_ALLOC_BLOCKS ? preAllocMD5Array : (MD5_t *) malloc(sizeof(MD5_t) * blocksToWriteSz);
	int blockIndex, writtenIndex, replyIndex;
    /*
        If partial 4k write
            md5 = control->get(block)
            if (md5)
                buffer = data->get(md5)
                memcpy(newbuf, buffer, 4k)
                memcpy(newbuf, curbuf, wherever)
            else
                bzero(newbuf, 4k)
                memcpy(newbuf, curbuf, wherever)
       else
            Delete old md5 from data server (if ref count == 1)
        Compute new_md5 for newbuf
        data->setnx(new_md5, newbuf)
        control->set(block, new_md5)
    */
    //LOG("ldb_write sector_num:%ld, nb_sectors:%d", sector_num, nb_sectors);

    int localResult;
    NonAlignedCopy a;
    NonAlignedCopyInit(&a, sector_num * SECTOR_SIZE, nb_sectors * SECTOR_SIZE, LDB_BLKSIZE);
    blockIndex = writtenIndex = replyIndex = 0;
    long curPage = -1; // current page this block
    long prevPage;
    page_md5_t page_md5;

    // Pipeline data writing process
    // - write data to ledisdb-data
    // - write metadata to ledisdb-meta
    while (NonAlignedCopyIsValid(&a))
    {
        ssize_t retOff = 0;
        ssize_t retSz = 0;
        NonAlignedCopyNext(&a, &retOff, &retSz);

        ssize_t relativeOff = retOff - (sector_num * SECTOR_SIZE);
        assert(relativeOff < nb_sectors * SECTOR_SIZE);
        const uint8_t* curBuf = buf + relativeOff;

		const uint8_t* actualBuf = curBuf; // actual data that we want to write

        prevPage = curPage;
        curPage = OFF_TO_PAGE(retOff);

        // handle if we move to different page:
        // - write metadata of previous page
        // - get metadata of current page
        if (prevPage != curPage) {
            // commit metadata of previous page
            if (prevPage != -1)
            {
                while (replyIndex < writtenIndex) {
                    localResult = DataCheckReplyPipeline(&es, smallMode, s, ldbIdsToWrite[replyIndex++]);
                    if (localResult != 0)
                    {
                        break;
                    }
                }
                if (localResult != 0)
                {
                    LOG("ldb_write():DataCheckReplyPipeline failed.result=%d",localResult);
                    result = -EIO;
                    break;
                }

                localResult = ldb_meta_update(prevPage, page_md5);
                if (localResult != 0)
                {
                    LOG("ldb_write()failed to write page metadata:%d, page=%ld", localResult, prevPage);
                    result = -EIO;
                    break;
                }
            }

            // get current page metadata
            //localResult = MetaPagesGetCmd(s, OFF_TO_PAGE(retOff), page_md5);
            localResult = ldb_meta_get(s->meta.context, OFF_TO_PAGE(retOff), page_md5);
            if (localResult != 0)
            {
                LOG("ldb_write() failed to get page metadata:%d", localResult);
                result = -EIO;
                break;
            }
        }

        // handle case where data size < 4k
        // - read old data (if any)
        // - update old data with new data
        // we use sync command without pipeline in this case
        if (retSz != LDB_BLKSIZE)
        {
	   		actualBuf = (const uint8_t*)malloc(LDB_BLKSIZE);
			bzero((char*)actualBuf, LDB_BLKSIZE);

			MD5_t md5;
            localResult = ldb_meta_pages_get_meta_block(retOff, curPage, page_md5, md5);

			if (localResult == 0)
			{
				// overlay old block, if any, onto the new block
				localResult = DataGetCmd(&es, s, md5, (char*)actualBuf);
				assert(localResult == 0);
				//LOG("RMW done at blk=%lu siz=%lu \n", BLOCK_NUMBER(retOff), retSz);
				s->readModifyWrites ++;
			}

			// current write may not start at 4K boundary
			memcpy((char*)actualBuf + (retOff % LDB_BLKSIZE), curBuf, retSz);
        }

        //Compute new_md5 for newbuf
        computeMD5((const char*)actualBuf, md5Array[blockIndex]);
		ldbIdsToWrite[blockIndex] = LDB_DATA_NUMBER_GET(md5Array[blockIndex]);

        //data->setnx(new_md5,
        localResult = DataSetCmdPipeline(&es, smallMode, s, ldbIdsToWrite[blockIndex], md5Array[blockIndex], (const char*)actualBuf);
        if (localResult != 0)
        {
            LOG("ldb_write():DataSetCmdPipeline failed.result=%d",localResult);
            result = -EIO;
            if (retSz != LDB_BLKSIZE)
            {
                free((char*)actualBuf);
            }
            break;
        }

        // flush command
        localResult = RedisWritePipelineFlush(&es, smallMode, s, ldbIdsToWrite[blockIndex]);
        if (localResult != 0) {
            LOG("ldb_write():meta redis flush failed");
            result = -EIO;
            if (retSz != LDB_BLKSIZE)
            {
                free((char*)actualBuf);
            }
            break;
        }
        writtenIndex++;

        //control->set(block, new_md5)
        ldb_meta_pages_set_meta_block(retOff, curPage, page_md5, md5Array[blockIndex]);


        //uint32_t crc = crc32c(0xffffffff, actualBuf, LDB_BLKSIZE);
        //LOG("write at blk=%lu siz=%lu checksum=%u\n", BLOCK_NUMBER(retOff), retSz, crc);

        int ret = tlog_write(s->tlog.context, BLOCK_NUMBER(retOff), md5Array[blockIndex], (const char *) actualBuf);
        if (ret != 0) {
            LOG("tlog_write failed:%d",ret);
            result = -EIO;
            if (retSz != LDB_BLKSIZE)
            {
                free((char*)actualBuf);
            }
            break;
        }
		if (retSz != LDB_BLKSIZE)
		{
			free((char*)actualBuf);
		}
		blockIndex++;
    }

    if (result != 0)
    {
        // Drain replies...
        while (replyIndex < writtenIndex) {
            DataCheckReplyPipeline(&es, smallMode, s, ldbIdsToWrite[replyIndex]);
        }
        // rollback tlog
        tlog_rollback(s->tlog.context, 0);
        // Done, free resources and return error
        if (blocksToWriteSz > PRE_ALLOC_BLOCKS) {
            free(ldbIdsToWrite);
            free(md5Array);
        }
        return result;
    }

    while (replyIndex < writtenIndex) {
        localResult = DataCheckReplyPipeline(&es, smallMode, s, ldbIdsToWrite[replyIndex++]);
        if (localResult != 0)
        {
            LOG("ldb_write():DataCheckReplyPipeline failed.result=%d",localResult);
            result = -EIO;
            break;
        }
    }
    if (result != 0)
    {
        // Drain replies...
        while (replyIndex < writtenIndex) {
            DataCheckReplyPipeline(&es, smallMode, s, ldbIdsToWrite[replyIndex++]);
        }
        // rollback tlog
        tlog_rollback(s->tlog.context, 0);
        // Done, free resources and return error
        if (blocksToWriteSz > PRE_ALLOC_BLOCKS) {
            free(ldbIdsToWrite);
            free(md5Array);
        }
        return result;
    }

    localResult = ldb_meta_update(curPage, page_md5);
    if (localResult != 0)
    {
        LOG("ldb_write()failed to write page metadata:%d for page:%ld", localResult,curPage);
        result = -EIO;
        // rollback tlog
        tlog_rollback(s->tlog.context, 0);
        // Done, free resources and return error
        if (blocksToWriteSz > PRE_ALLOC_BLOCKS) {
            free(ldbIdsToWrite);
            free(md5Array);
        }
        return result;
    }
    // commit tlog
    localResult = tlog_commit(s->tlog.context);
    if (localResult != 0)
    {
        LOG("ldb_write() failed: tlog_commit()");
        result = -EIO;
        // rollback tlog
        tlog_rollback(s->tlog.context, 0);
        // Done, free resources and return error
        if (blocksToWriteSz > PRE_ALLOC_BLOCKS) {
            free(ldbIdsToWrite);
            free(md5Array);
        }
        return result;
    }

    // OK!
    // Free resources
    if (blocksToWriteSz > PRE_ALLOC_BLOCKS) {
        free(ldbIdsToWrite);
        free(md5Array);
    }
    return result;
}

/*
static int ldb_co_readv(BlockDriverState *bs, int64_t sector_num,
                        int nb_sectors, QEMUIOVector *qiov)
{
    //LDBState *s = bs->opaque;

    //return ldb_client_session_co_readv(&s->client, sector_num,
                                       //nb_sectors, qiov);
}

static int ldb_co_writev(BlockDriverState *bs, int64_t sector_num,
                         int nb_sectors, QEMUIOVector *qiov)
{
    LDBState *s = bs->opaque;

    return ldb_client_session_co_writev(&s->client, sector_num,
                                        nb_sectors, qiov);
}
*/

/*
nbd.c write sync
if (qemu_in_coroutine())
{
    return qemu_co_send(fd, buffer, size);
    // qemu-common.h, qemu-coroutine-io.c
}

while (offset < size)
{
    read :qemu_recv(fd, buffer, size, 0)
    write:send(fd, buffer, size, 0)
}

static int ldb_co_flush(BlockDriverState *bs)
{
    LDBState *s = bs->opaque;

    return ldb_client_session_co_flush(&s->client);
}

static int ldb_co_discard(BlockDriverState *bs, int64_t sector_num,
                          int nb_sectors)
{
    LDBState *s = bs->opaque;

    return ldb_client_session_co_discard(&s->client, sector_num,
                                         nb_sectors);
}

*/

static void ldb_close(BlockDriverState *bs)
{
	int i;
	struct errstruct es;
    LDBState *s = bs->opaque;

    LOG("closed connection");

	for (i = 0; i < 256; i++) {
		sharedconn_release(s->data[i].context, &es);
        free(s->data[i].host);
	}
    redisFree(s->meta.context);
    tlog_close(s->tlog.context);
    free(s->meta.host);

#if (LOGME)
    fclose(s->fp);
#endif
}

static int64_t ldb_getlength(BlockDriverState *bs)
{
    LDBState *s = bs->opaque;
    (void)s;

    return  VOL_SIZE;
}

/*
static void ldb_detach_aio_context(BlockDriverState *bs)
{
    LDBState *s = bs->opaque;

    ldb_client_session_detach_aio_context(&s->client);
}

static void ldb_attach_aio_context(BlockDriverState *bs,
                                   AioContext *new_context)
{
    LDBState *s = bs->opaque;

    ldb_client_session_attach_aio_context(&s->client, new_context);
}
*/

static void ldb_refresh_filename(BlockDriverState *bs)
{
    LDBState *s = bs->opaque;
    (void) s;

}

static BlockDriver bdrv_ldb = {
    .format_name                = "ldb",
    .protocol_name              = "ldb",
    .instance_size              = sizeof(LDBState),
    //.bdrv_needs_filename        = true,
    .bdrv_parse_filename        = ldb_parse_filename,
    .bdrv_file_open             = ldb_open,
    .bdrv_read                  = ldb_read,
    .bdrv_write                 = ldb_write,
    .bdrv_close                 = ldb_close,
    //.bdrv_co_writev             = ldb_co_writev,
    //.bdrv_co_readv              = ldb_co_readv,
    //.bdrv_co_flush_to_os        = ldb_co_flush,
    //.bdrv_co_discard            = ldb_co_discard,
    .bdrv_getlength             = ldb_getlength,
    //.bdrv_detach_aio_context    = ldb_detach_aio_context,
    //.bdrv_attach_aio_context    = ldb_attach_aio_context,
    .bdrv_refresh_filename      = ldb_refresh_filename,
    //.create_opts                = ldb_create_opts,
};

// bdrv_probe, bdrv_reopen, bdrv_rebind,
// bdrv_get_info, bdrv_check,
// bdrv_get_allocated_file_size,
// bdrv_truncate
// bdrv_flush_to_disk
// Does REDIS have async write/read


static void bdrv_ldb_init(void)
{
    bdrv_register(&bdrv_ldb);
}

block_init(bdrv_ldb_init);
