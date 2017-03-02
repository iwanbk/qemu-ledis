/*
 * ldb_sc.c
 *
 *  Created on: Feb 6, 2015
 *      Author: Arne Tossens
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include "ldb_sc.h"
#include "timeval.h"

enum cmd_type { CMD_MSET, CMD_STOP };

struct command_common {
    enum cmd_type type;
};

struct command_stop {
    enum cmd_type type; // CMD_STOP
};

struct command_mset {
    enum cmd_type type; // CMD_MSET
    struct vararg *va;
};

static struct sharedconn **sc;
static int sc_count;
static redisReply sc_reply_ok = { REDIS_REPLY_INTEGER };

static int hostname_to_ip(char *hostname, char *ip) {
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_in *h;
    int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; // use AF_INET6 to force IPv6
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(hostname, "http", &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    // loop through all the results and connect to the first we can
    for (p = servinfo; p != NULL; p = p->ai_next) {
        h = (struct sockaddr_in *) p->ai_addr;
        strcpy(ip, inet_ntoa(h->sin_addr));
    }

    freeaddrinfo(servinfo); // all done with this structure
    return 0;
}

static struct sharedconn *doConnect(struct errstruct *esp, char *ip, int port, enum conn_type type) {
    struct sharedconn *scp = malloc(sizeof(struct sharedconn));
    memset(scp, 0, sizeof(struct sharedconn));
    esp->err = 0;
    esp->errstr[0] = '\0';
    scp->context = redisConnect(ip, port);
    if (scp->context == NULL) {
        free(scp);
        return NULL;
    }
    esp->err = scp->context->err;
    strcpy(esp->errstr, scp->context->errstr);
    return scp;
}

static void doDisconnect(struct sharedconn *scp) {
    redisFree(scp->context);
    free(scp);
}

static int doAppendCommand(struct sharedconn *scp, struct errstruct *esp, const char *format, va_list ap) {
    int retval;
    esp->err = 0;
    esp->errstr[0] = '\0';
    retval = redisvAppendCommand(scp->context, format, ap);
    if (scp->context->err != 0) {
        esp->err = scp->context->err;
        strcpy(esp->errstr, scp->context->errstr);
    }
    return retval;
}

// Called multi-threaded
static void *doCommand(struct sharedconn *scp, struct errstruct *esp, const char *format, va_list ap) {
    void *reply;
    pthread_mutex_lock(&scp->mutex);
    esp->err = 0;
    esp->errstr[0] = '\0';
    reply = redisvCommand(scp->context, format, ap);
    if (reply == NULL) {
        esp->err = scp->context->err;
        strcpy(esp->errstr, scp->context->errstr);
    }
    pthread_mutex_unlock(&scp->mutex);
    return reply;
}

static int doBufferWrite(struct sharedconn *scp, struct errstruct *esp) {
    int retval;
    int wdone = 0;
    esp->err = 0;
    esp->errstr[0] = '\0';
    do {
        retval = redisBufferWrite(scp->context, &wdone);
        if (retval != 0) {
            esp->err = scp->context->err;
            strcpy(esp->errstr, scp->context->errstr);
            break;
        }
    } while (!wdone);
    return retval;
}

static int doGetReply(struct sharedconn *scp, struct errstruct *esp, void **reply) {
    int retval;
    esp->err = 0;
    esp->errstr[0] = '\0';
    retval = redisGetReply(scp->context, (void **) reply);
    if (retval != 0) {
        esp->err = scp->context->err;
        strcpy(esp->errstr, scp->context->errstr);
    }
    return retval;
}

// Called multi-threaded
static void *doCommandVararg(struct sharedconn *scp, struct errstruct *esp, struct vararg *va) {
    void *reply;
    pthread_mutex_lock(&scp->mutex);
    esp->err = 0;
    esp->errstr[0] = '\0';
    reply = redisCommandArgv(scp->context, vararg_argc(va), vararg_argv(va), vararg_argvlen(va)); // reply->type is REDIS_REPLY_STATUS
    if (reply == NULL) {
        esp->err = scp->context->err;
        strcpy(esp->errstr, scp->context->errstr);
    }
    pthread_mutex_unlock(&scp->mutex);
    return reply;
}

static void *popthread(void *arg) {
    struct errstruct es;
    struct sharedconn *scp = (struct sharedconn *) arg;
    void *data;
    struct command_common *cmd;
    struct command_mset *cmd_mset;
    redisReply *reply;
    int exit = 0;
    int i;

    for (;;) {
        blqueue_pop(&scp->commands, &data);
        cmd = (struct command_common *) data;
        switch (cmd->type) {
        case CMD_MSET:
            cmd_mset = (struct command_mset *) data;
            reply = doCommandVararg(scp, &es, cmd_mset->va);
            if (reply == NULL) {
                // For all elements of the batch, reply with a not-OK message that contains the error message
                int reply_nok_len = strlen(es.errstr);
                for (i = 0; i < cmd_mset->va->elements; i++) {
                    struct redisReply *reply_nok = malloc(sizeof(struct redisReply));
                    memset(reply_nok, 0, sizeof(struct redisReply));
                    reply_nok->type = REDIS_REPLY_ERROR;
                    reply_nok->len = reply_nok_len;
                    reply_nok->str = strdup(es.errstr);
                    blqueue_push(&scp->replies, reply_nok);
                }
                fprintf(stderr, "(%s:%d) MSET(%d) error %d %s\n", scp->ip, scp->port, (int) cmd_mset->va->elements, es.err, es.errstr);
            }
            else {
                // For all elements of the batch, reply with our fixed OK message
                for (i = 0; i < cmd_mset->va->elements; i++) {
                    blqueue_push(&scp->replies, &sc_reply_ok);
                }
            }
            vararg_destroy(cmd_mset->va);
            freeReplyObject(reply);
            break;
        case CMD_STOP:
            fprintf(stderr, "(%s:%d) received stop request\n", scp->ip, scp->port);
            exit = 1;
            break;
        default:
            fprintf(stderr, "(%s:%d) received unknown command type %d\n", scp->ip, scp->port, cmd->type);
            break;
        }
        free(data);
        if (exit) {
            break;
        }
    }
    return NULL;
}

struct sharedconn *sharedconn_addref(struct errstruct *esp, char *hostname, int port, enum conn_type type) {
    char ip[100];
    int i;
    struct sharedconn *scp;
    hostname_to_ip(hostname, ip);
    // Share scp if ip/port/type already exists
    for (i = 0; i < sc_count; i++) {
        scp = sc[i];
        if (!strcmp(ip, scp->ip) && (port == scp->port) && (type == scp->type)) {
            scp->usages++;
            esp->err = 0;
            esp->errstr[0] = '\0';
            return scp;
        }
    }
    // Create new scp
    scp = doConnect(esp, ip, port, type);
    if (scp == NULL) {
        return NULL;
    }
    if (esp->err != 0) {
        return scp;
    }
    // Initialize scp
    scp->usages = 1;
    scp->ip = strdup(ip);
    scp->port = port;
    scp->type = type;
    pthread_mutex_init(&scp->mutex, NULL);
    blqueue_init(&scp->commands, 256);
    blqueue_init(&scp->replies, 256);
    pthread_create(&scp->popthread, NULL, popthread, scp);
    // Store new scp
    sc_count++;
    if (sc_count == 1) {
        sc = (struct sharedconn **) malloc(sizeof(struct sharedconn **));
    }
    else {
        sc = (struct sharedconn **) realloc(sc, sc_count * sizeof(struct sharedconn **));
    }
    sc[sc_count - 1] = scp;
    return scp;
}

void sharedconn_release(struct sharedconn *scpRelease, struct errstruct *esp) {
    int i;
    struct sharedconn *scp;
    struct command_stop *cmd_stop;

    // Connection failed
    if (scpRelease->usages == 0) {
        doDisconnect(scpRelease);
        return;
    }
    // Still shared
    scpRelease->usages--;
    if (scpRelease->usages > 0) {
        return;
    }
    // Last usage, find index
    for (i = 0; i < sc_count; i++) {
        scp = sc[i];
        if (scpRelease == scp) {
            break;
        }
    }
    // Free resources
    cmd_stop = malloc(sizeof(struct command_stop));
    cmd_stop->type = CMD_STOP;
    blqueue_push(&scp->commands, cmd_stop);
    pthread_join(scp->popthread, NULL);
    pthread_mutex_destroy(&scp->mutex);
    blqueue_destroy(&scp->commands);
    blqueue_destroy(&scp->replies);
    free(scpRelease->ip);
    doDisconnect(scpRelease);
    // Remove slot in array
    sc_count--;
    if (sc_count == 0) {
        free(sc);
        sc = NULL;
    }
    else {
        sc[i] = sc[sc_count];
        sc = (struct sharedconn **) realloc(sc, sc_count * sizeof(struct sharedconn **));
    }
}

int sharedconn_appendcommand(struct sharedconn *scp, struct errstruct *esp, const char *format, ...) {
    int retval = 0;
    va_list ap;
    va_start(ap, format);
    retval = doAppendCommand(scp, esp, format, ap);
    va_end(ap);
    return retval;
}

int sharedconn_bufferwrite(struct sharedconn *scp, struct errstruct *esp) {
    return doBufferWrite(scp, esp);
}

int sharedconn_getreply(struct sharedconn *scp, struct errstruct *esp, void **reply) {
    return doGetReply(scp, esp, reply);
}

void *sharedconn_command(struct sharedconn *scp, struct errstruct *esp, const char *format, ...) {
    void *retval;
    va_list ap;
    va_start(ap, format);
    retval = doCommand(scp, esp, format, ap);
    va_end(ap);
    return retval;
}

int sharedconn_append_keyvalue(struct sharedconn *scp, struct errstruct *esp, char *md5, const char *block, int batchsize) {
    if (scp->va == NULL) {
        scp->va = vararg_create(batchsize, "MSET");
    }
    return vararg_add_keyvalue(scp->va, md5, 16, (char *) block, 4096);
}

int sharedconn_flush_batch(struct sharedconn *scp, struct errstruct *esp, int batchsize) {
    esp->err = 0;
    esp->errstr[0] = '\0';
    if (scp->va == NULL) {
        return 0;
    }
    if (scp->va->elements >= batchsize) {
        struct command_mset *cmd = malloc(sizeof(struct command_mset));
        cmd->type = CMD_MSET;
        cmd->va = scp->va;
        blqueue_push(&scp->commands, cmd);
        scp->va = NULL;
    }
    return 0;
}

int sharedconn_popreply(struct sharedconn *scp, struct errstruct *esp, void **reply) {
    esp->err = 0;
    esp->errstr[0] = '\0';
    return blqueue_pop(&scp->replies, reply);
}

void sharedconn_freereply(struct sharedconn *scp, struct errstruct *esp, void *reply) {
    esp->err = 0;
    esp->errstr[0] = '\0';
    if (reply == &sc_reply_ok) {
        return;
    }
    freeReplyObject(reply);
}
