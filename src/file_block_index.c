/**
 *
 * @file    file_block_index
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/26 16:38:41
 */

#include <stdlib.h>
#include <string.h>

#include "file_block_index.h"

#define INFQ_DEFAULT_INDEX_CAPACITY     128
#define idx_in_fblock(fb, idx)          ((idx) >= (fb)->start_index && \
                                                 (idx) < (fb)->start_index + (fb)->ele_count)

int32_t
file_block_index_init(file_block_index_t *index)
{
    if (index == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    memset(index, 0, sizeof(file_block_index_t));
    index->blocks = (file_block_t **)malloc(sizeof(file_block_t *) * INFQ_DEFAULT_INDEX_CAPACITY);
    if (index->blocks == NULL) {
        INFQ_ERROR_LOG("failed to alloc mem for index array");
        return INFQ_ERR;
    }

    index->block_capacity = INFQ_DEFAULT_INDEX_CAPACITY;
    index->first = index->last = 0;

    return INFQ_OK;
}

int32_t
file_block_index_push(file_block_index_t *index, file_block_t *file_block)
{
    if (index == NULL || file_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_block_t    **blocks;

    // full
    if (index->last >= index->block_capacity) {
        // expand capacity
        blocks = (file_block_t **)realloc(
                index->blocks,
                sizeof(file_block_t *) * index->block_capacity * 2);
        if (blocks == NULL) {
            INFQ_ERROR_LOG("failed to alloc mem for index when expand capacity, capacity: %d",
                    index->block_capacity);
            return INFQ_ERR;
        }

        INFQ_INFO_LOG("index capacity expand, %d -> %d",
                index->block_capacity, index->block_capacity * 2);
        index->blocks = blocks;
        index->block_capacity *= 2;
    }

    index->blocks[index->last++] = file_block;

    return INFQ_OK;
}

int32_t
file_block_index_pop(file_block_index_t *index)
{
    if (index == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (index->first >= index->last) {
        INFQ_ERROR_LOG("index is empty");
        return INFQ_ERR;
    }

    index->blocks[index->first++] = NULL;

    return INFQ_OK;
}

int32_t
file_block_index_search(file_block_index_t *index, int64_t global_idx, file_block_t **file_block)
{
    if (index == NULL || file_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t         s = index->first, e = index->last - 1, mid;
    file_block_t    *block;

    *file_block = NULL;
    while (s <= e) {
        mid = (s + e) / 2;
        block = index->blocks[mid];
        if (idx_in_fblock(block, global_idx)) {
            *file_block = block;
            return INFQ_OK;
        }

        if (global_idx < block->start_index) {
            e = mid - 1;
        } else {
            s = mid + 1;
        }
    }

    INFQ_ERROR_LOG("file block contains the value indexed by '%lld' not found", global_idx);

    return INFQ_ERR;
}

void
file_block_index_destroy(file_block_index_t *index)
{
    if (index == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    if (index->blocks == NULL) {
        return;
    }

    free(index->blocks);
    index->blocks = NULL;
}
