#ifndef _LDB_META_H_
#define _LDB_META_H_

#include "hiredis/hiredis.h"
#include "ldb_md5.h"

#define PAGE_MD5_LEN (16 * 256)

#define BLOCK_NUMBER(off) (off/4096)
#define BLOCK_TO_PAGE(block) (block/256)
#define OFF_TO_PAGE(off) BLOCK_TO_PAGE(BLOCK_NUMBER(off))

typedef char page_md5_t[MD5_LEN * 256];
int ldb_meta_init(redisContext *ctx, int vol_id);
int ldb_meta_update(ssize_t key, page_md5_t pm);
int ldb_meta_get(redisContext *ctx, ssize_t key, page_md5_t pm);
int ldb_meta_pages_get_meta_block(ssize_t off, ssize_t page, page_md5_t page_md5, MD5_t md5);
void ldb_meta_pages_set_meta_block(ssize_t off, ssize_t page, page_md5_t page_md5, MD5_t md5);

#endif
