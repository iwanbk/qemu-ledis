/*
 * ldb_vararg.h
 *
 *  Created on: Feb 11, 2015
 *      Author: Arne Tossens
 */

#ifndef LDB_VARARG_H_
#define LDB_VARARG_H_

struct vararg {
    size_t maxElements;
    size_t elements;
    char **strings;
    size_t *lengths;
};

#define vararg_command(va) ((va)->strings[0])
#define vararg_commandLen(va) ((va)->lengths[0])

#define vararg_key(va, i) ((va)->strings[1 + (2 * (i))])
#define vararg_keyLen(va, i) ((va)->lengths[1 + (2 * (i))])

#define vararg_value(va, i) ((va)->strings[2 + (2 * (i))])
#define vararg_valueLen(va, i) ((va)->lengths[2 + (2 * (i))])

#define vararg_argc(va) (((va)->elements * 2) + 1)
#define vararg_argv(va) (const char **) ((va)->strings)
#define vararg_argvlen(va) ((va)->lengths)

#define vararg_clear_data(va) ((va)->elements = 0)

struct vararg *vararg_create(size_t maxElements, const char *command);
void vararg_destroy(struct vararg *va);
int vararg_add_keyvalue(struct vararg *va, char *key, size_t keyLen, char *value, size_t valueLen);

#endif /* LDB_VARARG_H_ */
