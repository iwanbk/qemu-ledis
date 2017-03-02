/*
 * ldb_int.h
 *
 *  Created on: Feb 9, 2015
 *      Author: Arne Tossens
 */

#ifndef LDB_INT_H_
#define LDB_INT_H_

#define LOGME 0

#include "hiredis/hiredis.h"
#include "ldb_tlog.h"

/**
 * Connection represents a connection to ledisdb.
 * Either ledisdb meta or ledisdb data
 */
typedef struct Connection
{
    redisContext* context;
    char *host;
    int port;
    int64_t missingReads;
    int64_t reads;
    int64_t writes;
} Connection;

typedef struct SharedConnection
{
    struct sharedconn *context;
    char *host;
    int port;
    int64_t missingReads;
    int64_t reads;
    int64_t writes;
#if (LOGME)
    FILE *fp;
#endif
} SharedConnection;

typedef struct Tlog
{
    tlogContext* context;
} Tlog;

typedef struct LDBState {
    SharedConnection data[256]; // we need 256 data conns because we will connect to 256 address, depend on our first byte value
    Connection meta;        // we only talk to 1 ledisdb-meta, depend on our vdisk id
    Tlog tlog;
    int64_t readModifyWrites;
#if (LOGME)
    FILE *fp;
#endif
} LDBState;

#endif /* LDB_INT_H_ */
