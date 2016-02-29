/**
 *
 * @file    file_block_index
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/25 16:30:24
 */

#ifndef COM_MOMO_INFQ_FILE_BLOCK_INDEX_H
#define COM_MOMO_INFQ_FILE_BLOCK_INDEX_H

#include "infq.h"
#include "file_block.h"

typedef struct _file_block_index_t {
    file_block_t    **blocks;
    int32_t         block_capacity;
    int32_t         first;
    int32_t         last;   // point to the position to next push
} file_block_index_t;

int32_t file_block_index_init(file_block_index_t *index);
int32_t file_block_index_push(file_block_index_t *index, file_block_t *file_block);
int32_t file_block_index_pop(file_block_index_t *index);
int32_t file_block_index_search(file_block_index_t *index, int64_t global_idx, file_block_t **file_block);
void file_block_index_destroy(file_block_index_t *index);

#endif
