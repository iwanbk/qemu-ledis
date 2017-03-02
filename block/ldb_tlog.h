#ifndef _LDB_TLOG_H_
#define _LDB_TLOG_H_

extern const char *TLOG_FOLDER_ROOT;

typedef struct tlogContext {
    int err;
    char errstr[128];
    int vol_id;
    int snapshot_id;
    char log_name[100];
    FILE *fp_tlog;
    long last_commit;
    long long file_nr;
} tlogContext;

tlogContext *tlog_open(int vol_id, int snapshot_id);
int tlog_init(tlogContext *tlog_ctx);
int tlog_init_next(tlogContext *tlg_ctx);
int tlog_write(tlogContext *tlog_ctx, int64_t lbapos, char *md5, const char *data);
void tlog_close(tlogContext *tlog_ctx);
int tlog_commit(tlogContext *tlg_ctx);
int tlog_rollback(tlogContext *tlg_ctx, int close);

#define TLOG_ERR_COULD_NOT_UPDATE_INDEX   1
#define TLOG_ERR_COULD_NOT_CREATE_EXTENT  2
#define TLOG_ERR_COULD_NOT_INIT_DIRECTORY 3

#endif
