#include <stdlib.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>

#include "hiredis/hiredis.h"

#include "ldb_tlog.h"
#include "ldb_md5.h"
#include "blqueue.h"

#define TLOG_ON 1
#define TLOG_VERSION 1
const char *TLOG_FOLDER_ROOT = "/home/arne/tlog";

#define LOG(msg, ...) do { \
    fprintf(stderr, "%s:%s():L%d: " msg "\n", \
    __FILE__, __FUNCTION__, __LINE__, ## __VA_ARGS__); \
    } while (0)

enum item_type {
    SCO_HEADER, SCO_DATA, SCO_COMMIT
};

struct SCOHeader {
    enum item_type type; // SCO_HEADER
    char eye_catcher[8];
    int32_t volid;
    int32_t snapshotid;
    time_t epoch;
    int32_t version;
};

struct SCOData {
    enum item_type type; // SCO_DATA
    char eye_catcher[8];
    int64_t lbapos;
    char md5[16];
    char data[4096];
};

struct SCOCommit {
    enum item_type type; // SCO_COMMIT
    char eye_catcher[8];
    struct timeval time_commit;
};

#define MAX_TLOG_SIZE (16L * 1024L * 1024L)

static struct SCOHeader scoHeader;
static struct SCOData scoData;
static struct SCOCommit scoCommit;

static long long find_index(int vol_id) {
    long long nr;
    char buf[100];
    FILE *fp;

    sprintf(buf, "%s/vol%d/index", TLOG_FOLDER_ROOT, vol_id);
    fp = fopen(buf, "r");
    if (fp == NULL) {
        return -1;
    }
    fscanf(fp, "%lld", &nr);
    fclose(fp);
    return nr;
}

static int update_index(int vol_id, long long nr) {
    char buf[100];
    FILE *fp;

    sprintf(buf, "%s/vol%d/index", TLOG_FOLDER_ROOT, vol_id);
    fp = fopen(buf, "w");
    if (fp == NULL) {
        return -1;
    }
    fprintf(fp, "%lld", nr);
    fclose(fp);
    return 0;
}

// check if a folder exists
static int tlog_folder_check(const char *path) {
    DIR *dir;
    
    if(!(dir = opendir(path))) {
        if(errno == ENOENT)
            return 0;
        
        return errno;
    }
    
    closedir(dir);
    
    return 1;
}

tlogContext *tlog_open(int vol_id, int snapshot_id) {
    int retval;
    char buf[128];
    
    // building the context
    tlogContext *tlg_ctx = malloc(sizeof(tlogContext));
    memset(tlg_ctx, 0, sizeof(tlogContext));
    if (TLOG_ON == 0) {
        return 0;
    }
    
    // checking paths
    if(!tlog_folder_check(TLOG_FOLDER_ROOT)) {
        LOG("%s not found, creating it", TLOG_FOLDER_ROOT);
        
        if(mkdir(TLOG_FOLDER_ROOT, 0777)) {
            LOG("mkdir failed (%d)", errno);
            
            tlg_ctx->err = TLOG_ERR_COULD_NOT_INIT_DIRECTORY;
            strcpy(tlg_ctx->errstr, "Could not tlog directory");
            return tlg_ctx;
        }
    }
    
    snprintf(buf, sizeof(buf), "%s/vol%d", TLOG_FOLDER_ROOT, vol_id);
    
    if(!tlog_folder_check(buf)) {
        LOG("%s not found, creating it", buf);
        
        if(mkdir(buf, 0777)) {
            LOG("mkdir failed (%d)", errno);
            
            tlg_ctx->err = TLOG_ERR_COULD_NOT_INIT_DIRECTORY;
            strcpy(tlg_ctx->errstr, "Could not create tlog volid directory");
            return tlg_ctx;
        }
    }
    
    // fill context
    tlg_ctx->vol_id = vol_id;
    tlg_ctx->snapshot_id = snapshot_id;
    tlg_ctx->file_nr = find_index(vol_id);
    if (tlg_ctx->file_nr == -1) {
        retval = update_index(vol_id, 0L);
        if (retval != 0) {
            tlg_ctx->err = TLOG_ERR_COULD_NOT_UPDATE_INDEX;
            strcpy(tlg_ctx->errstr, "Could not create or update index (open)");
            return tlg_ctx;
        }
    }
    scoHeader.type = SCO_HEADER;
    scoHeader.version = TLOG_VERSION;
    scoHeader.volid = vol_id;
    scoHeader.snapshotid = snapshot_id;
    memcpy(scoHeader.eye_catcher, "{HEADER}", 8);
    scoData.type = SCO_DATA;
    memcpy(scoData.eye_catcher, "{DATA  }", 8);
    scoCommit.type = SCO_COMMIT;
    memcpy(scoCommit.eye_catcher, "{COMMIT}", 8);
    return tlg_ctx;
}

void tlog_close(tlogContext *tlg_ctx) {
    if (TLOG_ON == 0) {
        return;
    }
    
    tlog_rollback(tlg_ctx, 1);
    tlg_ctx->fp_tlog = NULL;
    free(tlg_ctx);
}

static int tlog_append(tlogContext *tlg_ctx) {
    // checking empty file
    fseek(tlg_ctx->fp_tlog, 0, SEEK_END);
    
    if(ftell(tlg_ctx->fp_tlog) == 0)
        return 1;
    
    // not empty, checking content
    rewind(tlg_ctx->fp_tlog);
    
    // checking header consistance
    if(fread(&scoHeader, sizeof(scoHeader), 1, tlg_ctx->fp_tlog) != 1) {
        LOG("cannot read header, skipping");
        return 0;
    }
    
    if(scoHeader.version != TLOG_VERSION) {
        LOG("current tlog version mismatch, skipping");
        return 0;
    }
    
    // checking if the file is finished by a commit
    fseek(tlg_ctx->fp_tlog, - sizeof(scoCommit), SEEK_END);
    
    if(fread(&scoCommit, sizeof(scoCommit), 1, tlg_ctx->fp_tlog) != 1) {
        LOG("something wrong with previous tlog read, skipping");
        return 0;
    }
    
    if(scoCommit.type != SCO_COMMIT) {
        LOG("last record don't seems to be a commit one, skipping");
        return 0;
    }
    
    tlg_ctx->last_commit = ftell(tlg_ctx->fp_tlog);
    LOG("current tlog last commit: %ld\n", tlg_ctx->last_commit);
    
    scoHeader.epoch = time(NULL);
    
    return 1;
}

int tlog_init(tlogContext *tlg_ctx) {
    if (TLOG_ON == 0) {
        return 0;
    }
    
    // tlog already exists, trying append
    if(tlg_ctx->file_nr != -1) {
        LOG("tlog already exists, appending");
        sprintf(tlg_ctx->log_name, "%s/vol%d/%lld", TLOG_FOLDER_ROOT, tlg_ctx->vol_id, tlg_ctx->file_nr);
        
        if((tlg_ctx->fp_tlog = fopen(tlg_ctx->log_name, "r+"))) {
            if(tlog_append(tlg_ctx))
                return 0;
        }
        
        LOG("cannot open previous tlog file, skipping");
    }
    
    return tlog_init_next(tlg_ctx);
}

int tlog_init_next(tlogContext *tlg_ctx) {
    // creating a new one
    sprintf(tlg_ctx->log_name, "%s/vol%d/%lld", TLOG_FOLDER_ROOT, tlg_ctx->vol_id, ++tlg_ctx->file_nr);
    tlg_ctx->fp_tlog = fopen(tlg_ctx->log_name, "w");
    if (tlg_ctx->fp_tlog == NULL) {
        tlg_ctx->err = TLOG_ERR_COULD_NOT_CREATE_EXTENT;
        strcpy(tlg_ctx->errstr, "Could not create extent (init)");
        return TLOG_ERR_COULD_NOT_CREATE_EXTENT;
    }
    update_index(tlg_ctx->vol_id, tlg_ctx->file_nr);
    // write header
    scoHeader.epoch = time(NULL);
    if (fwrite(&scoHeader, sizeof(scoHeader), 1, tlg_ctx->fp_tlog) != 1) {
        return -1;
    }
    tlg_ctx->last_commit = 0L;
    return 0;
}

int tlog_write(tlogContext *tlg_ctx, int64_t lbapos, char *md5, const char *data) {
    if (TLOG_ON == 0) {
        return 0;
    }
    scoData.lbapos = lbapos;
    memcpy(scoData.md5, md5, 16);
    memcpy(scoData.data, data, 4096);
    if (fwrite(&scoData, sizeof(scoData), 1, tlg_ctx->fp_tlog) != 1) {
        return -1;
    }
    return 0;
}

int tlog_commit(tlogContext *tlg_ctx) {
    long pos;

    if (TLOG_ON == 0) {
        return 0;
    }
    gettimeofday(&scoCommit.time_commit, NULL);
    if (fwrite(&scoCommit, sizeof(scoCommit), 1, tlg_ctx->fp_tlog) != 1) {
        return -1;
    }
    if (fflush(tlg_ctx->fp_tlog) != 0) {
        return -1;
    }
    pos = ftell(tlg_ctx->fp_tlog);
    if (pos >= MAX_TLOG_SIZE) {
        fclose(tlg_ctx->fp_tlog);
        return tlog_init_next(tlg_ctx);
    }
    tlg_ctx->last_commit = pos;
    return 0;
}

int tlog_rollback(tlogContext *tlg_ctx, int close) {
    if (TLOG_ON == 0) {
        return 0;
    }
    
    if(tlg_ctx->fp_tlog) {
        fclose(tlg_ctx->fp_tlog);
    }
        
    if (truncate(tlg_ctx->log_name, tlg_ctx->last_commit) != 0) {
        return -1;
    }
    if (!close) {
        tlg_ctx->fp_tlog = fopen(tlg_ctx->log_name, "a");
        if (tlg_ctx->fp_tlog == NULL) {
            return -1;
        }
    }
    return 0;
}

