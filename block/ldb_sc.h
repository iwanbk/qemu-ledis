/*
 * ldb_sc.h
 *
 *  Created on: Feb 6, 2015
 *      Author: Arne Tossens
 */

#ifndef LDB_SC_H_
#define LDB_SC_H_

#include <stdio.h>
#include "hiredis/hiredis.h"
#include "ldb_int.h"
#include "ldb_vararg.h"
#include "blqueue.h"

enum conn_type { DATA_TYPE, METADATA_TYPE, TLOG_TYPE };

struct sharedconn {
    redisContext *context;
    char *ip;
    int port;
    enum conn_type type;
    int usages;
    pthread_mutex_t mutex;
    pthread_t popthread;
    struct vararg *va;
    struct _blqueue commands;
    struct _blqueue replies;
#if (LOGME)
    FILE *fp;
#endif
};

struct errstruct {
    int err;
    char errstr[128]; // matched to hiredis redisContext.errstr
};

struct sharedconn *sharedconn_addref(struct errstruct *esp, char *hostname, int port, enum conn_type type);
void sharedconn_release(struct sharedconn *scp, struct errstruct *esp);
int sharedconn_appendcommand(struct sharedconn *scp, struct errstruct *esp, const char *format, ...);
int sharedconn_bufferwrite(struct sharedconn *scp, struct errstruct *esp);
int sharedconn_getreply(struct sharedconn *scp, struct errstruct *esp, void **reply);
void *sharedconn_command(struct sharedconn *scp, struct errstruct *esp, const char *format, ...);
void sharedconn_freereply(struct sharedconn *scp, struct errstruct *esp, void *reply);
int sharedconn_append_keyvalue(struct sharedconn *scp, struct errstruct *esp, char *md5, const char *block, int batchsize);
int sharedconn_flush_batch(struct sharedconn *scp, struct errstruct *esp, int batchsize);
int sharedconn_popreply(struct sharedconn *scp, struct errstruct *esp, void **reply);

#endif /* LDB_SC_H_ */
