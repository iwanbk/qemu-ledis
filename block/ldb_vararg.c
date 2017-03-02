/*
 * ldb_vararg.c
 *
 *  Created on: Feb 11, 2015
 *      Author: Arne Tossens
 */

#include <stdlib.h>
#include <string.h>
#include "ldb_vararg.h"

struct vararg *vararg_create(size_t maxElements, const char *command) {
    struct vararg *va = malloc(sizeof(struct vararg));
    int nrAlloc = (maxElements * 2) + 1;
    memset(va, 0, sizeof(struct vararg));
    va->strings = malloc(nrAlloc * sizeof(char *));
    memset(va->strings, 0, nrAlloc * sizeof(char *));
    va->lengths = malloc(nrAlloc * sizeof(size_t *));
    memset(va->lengths, 0, nrAlloc * sizeof(size_t *));
    va->maxElements = maxElements;
    vararg_command(va) = (char *) command;
    vararg_commandLen(va) = strlen(command);
    return va;
}

void vararg_destroy(struct vararg *va) {
    free(va->strings);
    free(va->lengths);
    free(va);
}

int vararg_add_keyvalue(struct vararg *va, char *key, size_t keyLen, char *value, size_t valueLen) {
    if (va->elements == va->maxElements) {
        return -1;
    }
    vararg_key(va, va->elements) = key;
    vararg_keyLen(va, va->elements) = keyLen;
    vararg_value(va, va->elements) = value;
    vararg_valueLen(va, va->elements) = valueLen;
    va->elements++;
    return 0;
}


