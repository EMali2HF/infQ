/**
 *
 * @file    mem_queue
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 10:11:40
 */

#include <stdlib.h>
#include <string.h>

#include "mem_queue.h"
#include "infq.h"

#define idx_in_mblock(mb, idx)      ((idx) >= (mb)->start_index && \
                                             (idx) < (mb)->start_index + (mb)->ele_count)

mem_block_t* search_block_by_idx(mem_queue_t *mem_queue, int64_t idx);

int32_t
mem_queue_init(mem_queue_t *mem_queue, int32_t block_num, int32_t block_size)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    memset(mem_queue, 0, sizeof(mem_queue_t));

    mem_queue->block_num = block_num;
    mem_queue->blocks = (mem_block_t **)malloc(sizeof(mem_block_t *) * block_num);
    if (mem_queue->blocks == NULL) {
        INFQ_ERROR_LOG("failed to alloc memory for blocks array, block num: %d", block_num);
        return INFQ_ERR;
    }

    // make sure that mem block size can be exactly divided by 8.
    block_size = (block_size + 7) & (~INFQ_PADDING_MASK);

    for (int32_t i = 0; i < block_num; i++) {
        mem_queue->blocks[i] = mem_block_init(block_size);
        if (mem_queue->blocks[i] == NULL) {
            INFQ_ERROR_LOG("failed to init and alloc memory block");
            goto failed;
        }
    }

    mem_queue->min_idx = INFQ_UNDEF;
    mem_queue->max_idx = INFQ_UNDEF;

    return INFQ_OK;

failed:
    mem_queue_destroy(mem_queue);
    return INFQ_ERR;
}

int32_t
mem_queue_push(mem_queue_t *mem_queue, int64_t ele_idx, void *data, int32_t size)
{
    if (mem_queue == NULL || data == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

#ifdef D_ASSERT
    INFQ_ASSERT(mem_queue->max_idx - mem_queue->min_idx == mem_queue->ele_count,
            "index range and count of queue aren't match, min: %lld,"
            "max: %lld, first: %d, last: %d, count: %d",
            mem_queue->min_idx,
            mem_queue->max_idx,
            mem_queue->first_block,
            mem_queue->last_block,
            mem_queue->ele_count);
#endif

    // double check
    if (mem_queue_full(mem_queue)) {
        INFQ_ERROR_LOG("queue is full, block idx: [%d, %d], ele idx: [%d, %d]",
                mem_queue->first_block,
                mem_queue->last_block,
                mem_queue->min_idx,
                mem_queue->max_idx);
        return INFQ_ERR;
    }

    // fetch the last block
    mem_block_t *block = last_block(mem_queue);
    if (block == NULL) {
        INFQ_ERROR_LOG("block is NULL, last index: %d", mem_queue->last_block);
        return INFQ_ERR;
    }

    // init start index of block
    if (block->start_index == INFQ_UNDEF) {
        block->start_index = ele_idx;
    }

    // make sure the block has enough space
    if (mem_block_avail(block) < mem_block_ele_size(size)) {
        // TODO: make sure the next block is not in the dumping state

        mem_queue->last_block = (mem_queue->last_block + 1) % mem_queue->block_num;
        block = last_block(mem_queue);
        mem_block_reset(block, ele_idx);

        // one block full, call the callback function
        if (mem_queue->push_blk_cb != NULL && mem_queue->push_blk_cb_arg != NULL) {
            mem_queue->push_blk_cb(mem_queue->push_blk_cb_arg);
        }

        // the block queue is full
        if (mem_queue_full(mem_queue)) {
            INFQ_ERROR_LOG("queue is full, first block: %d, last block: %d, block num: %d"
                    ", min: %lld, max: %lld",
                    mem_queue->first_block,
                    mem_queue->last_block,
                    mem_queue->block_num,
                    mem_queue->min_idx,
                    mem_queue->max_idx);
            return INFQ_ERR;
        }
    }

    if (mem_block_push(block, data, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to push data to memory block, data size: %d, avail size: %d, "
                "first block: %d, last block: %d",
                size,
                mem_block_avail(block),
                mem_queue->first_block,
                mem_queue->last_block);
        return INFQ_ERR;
    }

    mem_queue->ele_count++;
    if (ele_idx + 1 > mem_queue->max_idx || mem_queue->max_idx == INFQ_UNDEF) {
        // exclude boundary
        mem_queue->max_idx = ele_idx + 1;
    }
    if (mem_queue->min_idx == INFQ_UNDEF) {
        mem_queue->min_idx = ele_idx;
    }

    return INFQ_OK;
}

int32_t
mem_queue_pop(mem_queue_t *mem_queue, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_queue == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void      *dataptr;

    if (mem_queue_pop_zero_cp(mem_queue, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop by zero copy");
        return INFQ_ERR;
    }

    if (*sizeptr == 0) {
        return INFQ_OK;
    }

    if (buf_size < *sizeptr) {
        INFQ_ERROR_LOG("buffer is not enough, buf size: %d, expect: %d",
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
mem_queue_pop_zero_cp(mem_queue_t *mem_queue, const void **dataptr, int32_t *sizeptr)
{
    if (mem_queue == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    // queue is empty
    if (mem_queue_empty(mem_queue)) {
        *dataptr = NULL;
        *sizeptr = 0;
        return INFQ_OK;
    }

#ifdef D_ASSERT
    INFQ_ASSERT(mem_queue->max_idx - mem_queue->min_idx == mem_queue->ele_count,
            "index range and count of queue aren't match, min: %lld,"
            "max: %lld, first: %d, last: %d, count: %d",
            mem_queue->min_idx,
            mem_queue->max_idx,
            mem_queue->first_block,
            mem_queue->last_block,
            mem_queue->ele_count);

#endif

    // NOTICE: 默认集成到redis里，只有在push元素之后才会创建InfQ，所以当pop时，
    //      已经push过元素，min_idx不可能为INFQ_UNDEF
    INFQ_ASSERT(mem_queue->min_idx != INFQ_UNDEF,
            "invalid min idx, min: %d, max: %d, first: %d, last: %d, count: %d, first blk count: %d",
            mem_queue->min_idx,
            mem_queue->max_idx,
            mem_queue->first_block,
            mem_queue->last_block,
            mem_queue->ele_count,
            first_block(mem_queue)->ele_count);

    mem_block_t *block = first_block(mem_queue);
    if (block == NULL) {
        INFQ_ERROR_LOG("memory block is NULL");
        return INFQ_ERR;
    }

    if (mem_block_pop_zero_cp(block, dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop data from memory block");
        return INFQ_ERR;
    }

    // an block is empty, call the callback function
    if (mem_block_empty(block) && mem_queue->pop_blk_cb != NULL
            && mem_queue->pop_blk_cb_arg != NULL) {
        mem_queue->pop_blk_cb(mem_queue->pop_blk_cb_arg, block);
    }

    // make 'first_block' point to the next block
    if (mem_block_empty(block) && !mem_queue_empty(mem_queue)) {
        mem_queue->first_block = (mem_queue->first_block + 1) % mem_queue->block_num;

        INFQ_DEBUG_LOG("pop new block, fileno: %d, min: %lld, max: %lld, count: %d, f: %d, l: %d,"
                "first count: %d, last count: %d, first start: %lld, last start: %lld",
                first_block(mem_queue)->file_block_no,
                mem_queue->min_idx,
                mem_queue->max_idx,
                mem_queue->ele_count,
                mem_queue->first_block,
                mem_queue->last_block,
                first_block(mem_queue)->ele_count,
                last_block(mem_queue)->ele_count,
                first_block(mem_queue)->start_index,
                last_block(mem_queue)->start_index);

        if ((mem_queue->first_block + 1) % mem_queue->block_num == mem_queue->last_block
                || mem_queue->first_block == mem_queue->last_block) {
            INFQ_DEBUG_LOG("debug min_idx, f: %d, l: %d, min: %lld, max: %lld, count: %d",
                    mem_queue->first_block,
                    mem_queue->last_block,
                    mem_queue->min_idx,
                    mem_queue->max_idx,
                    mem_queue->ele_count);
        }
    }

    if (*sizeptr == 0 && mem_queue->ele_count > 0) {
        INFQ_ERROR_LOG("queue has elements, but pop from block empty, min: %lld, max: %lld"
                ", count: %d",
                mem_queue->min_idx,
                mem_queue->max_idx,
                mem_queue->ele_count);
        return INFQ_ERR;
    }

    // mem queue is empty
    if (*sizeptr == 0) {
        return INFQ_OK;
    }

    if (mem_queue->min_idx < mem_queue->max_idx) {
        mem_queue->min_idx++;
    } else {
        INFQ_ERROR_LOG("unexpect index range, min idx: %lld, max idx: %lld, ele count: %d",
                mem_queue->min_idx,
                mem_queue->max_idx,
                mem_queue->ele_count);
        return INFQ_ERR;
    }

    mem_queue->ele_count--;

    return INFQ_OK;
}

int32_t
mem_queue_just_pop(mem_queue_t *mem_queue)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void  *dataptr;
    int32_t     size;

    if (mem_queue_pop_zero_cp(mem_queue, &dataptr, &size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call queue pop by zero copy");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
mem_queue_at(mem_queue_t *mem_queue, int64_t idx, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_queue == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void  *dataptr;

    if (mem_queue_at_zero_cp(mem_queue, idx, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call at by zero copy");
        return INFQ_ERR;
    }

    if (*sizeptr == 0) {
        return INFQ_OK;
    }

    if (*sizeptr > buf_size) {
        INFQ_ERROR_LOG("buffer is not enough, buf size: %d, expect: %d",
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
mem_queue_at_zero_cp(mem_queue_t *mem_queue, int64_t idx, const void **dataptr, int32_t *sizeptr)
{
    if (mem_queue == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

#ifdef D_ASSERT
    INFQ_ASSERT(mem_queue->max_idx - mem_queue->min_idx == mem_queue->ele_count,
            "index range and count of queue aren't match, min: %lld,"
            "max: %lld, first: %d, last: %d, count: %d",
            mem_queue->min_idx,
            mem_queue->max_idx,
            mem_queue->first_block,
            mem_queue->last_block,
            mem_queue->ele_count);

#endif

    if (mem_queue_empty(mem_queue)) {
        *dataptr = NULL;
        *sizeptr = 0;
        return INFQ_OK;
    }

    // make sure 'idx' is a valid, [min_idx, max_idx)
    if (idx < mem_queue->min_idx || idx >= mem_queue->max_idx) {
        INFQ_ERROR_LOG("invalid idx for at, idx: %lld, valid: [%lld, %lld)",
                idx,
                mem_queue->min_idx,
                mem_queue->max_idx);
        return INFQ_ERR;
    }

    // search the memory block own the value specified by the 'idx'
    mem_block_t *mem_block = search_block_by_idx(mem_queue, idx);
    if (mem_block == NULL) {
        INFQ_ERROR_LOG("failed to search block by index '%lld'", idx);
        return INFQ_ERR;
    }

    // search in memory block
    if (mem_block_at_zero_cp(mem_block, idx, dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to fetch data from memory block, index: %lld", idx);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
mem_queue_top(mem_queue_t *mem_queue, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_queue == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void  *dataptr;

    if (mem_queue_top_zero_cp(mem_queue, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call queue top by zero copy");
        return INFQ_ERR;
    }

    if (*sizeptr == 0) {
        return INFQ_OK;
    }

    if (buf_size < *sizeptr) {
        INFQ_ERROR_LOG("buffer is not enough, buf size: %d, expect: %d",
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
mem_queue_top_zero_cp(mem_queue_t *mem_queue, const void **dataptr, int32_t *sizeptr)
{
    if (mem_queue == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (mem_queue_empty(mem_queue)) {
        *dataptr = NULL;
        *sizeptr = 0;
        return INFQ_OK;
    }

    mem_block_t *block = first_block(mem_queue);
    if (block == NULL) {
        INFQ_ERROR_LOG("memory block is NULL");
        return INFQ_ERR;
    }

    if (mem_block_top_zero_cp(block, dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call top by zero copy");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
mem_queue_jump(mem_queue_t *mem_queue)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    // check current last block to see if it's an empty mem block,
    // quit the jump if it's empty
    mem_block_t *block = last_block(mem_queue);
    if (mem_block_empty(block)) {
        INFQ_DEBUG_LOG("mem block is empty, quit the mem queue jump");
        return INFQ_OK;
    }

    // check the memory queue to see if it's already full
    if (mem_queue_full(mem_queue)) {
        INFQ_ERROR_LOG("queue is full, first block: %d, last block: %d, block num: %d"
                ", min: %lld, max: %lld",
                mem_queue->first_block,
                mem_queue->last_block,
                mem_queue->block_num,
                mem_queue->min_idx,
                mem_queue->max_idx);
        return INFQ_ERR;
    }
    // jump to next memory block
    mem_queue->last_block = (mem_queue->last_block + 1) % mem_queue->block_num;

    // reset the last block
    block = last_block(mem_queue);
    mem_block_reset(block, INFQ_UNDEF);

    INFQ_DEBUG_LOG("queue jumped to next block, first block: %d, last block: %d"
            ", block num: %d",
            mem_queue->first_block,
            mem_queue->last_block,
            mem_queue->block_num);

    return INFQ_OK;
}

void
mem_queue_destroy(mem_queue_t *mem_queue)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    for (int32_t i = 0; i < mem_queue->block_num; i++) {
        if (mem_queue->blocks[i] == NULL) {
            continue;
        }
        mem_block_destroy(mem_queue->blocks[i]);
        mem_queue->blocks[i] = NULL;
    }

    if (mem_queue->blocks != NULL) {
        free(mem_queue->blocks);
        mem_queue->blocks = NULL;
    }
}

void
mem_queue_reset(mem_queue_t *mem_queue)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    mem_queue->ele_count = 0;
    mem_queue->first_block = mem_queue->last_block = 0;
    mem_queue->min_idx = mem_queue->max_idx = INFQ_UNDEF;
}

mem_block_t*
search_block_by_idx(mem_queue_t *mem_queue, int64_t idx)
{
    if (mem_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return NULL;
    }

    int32_t         b, e, m, last;
    mem_block_t     *block;

    b = mem_queue->first_block;
    // NOTICE: for pop queue the last block is always empty.
    //      for push queue the last block may be not empty.
    if (last_block(mem_queue)->ele_count == 0) {
        e = (mem_queue->last_block - 1) % mem_queue->block_num;
    } else {
        e = mem_queue->last_block;
    }
    if (mem_queue->first_block > mem_queue->last_block) {
        e += mem_queue->block_num;
    }
    last = e;
    while (b <= e) {
        m = (b + e) / 2;

        block = mem_queue->blocks[m % mem_queue->block_num];
        if (idx_in_mblock(block, idx)) {
            INFQ_ASSERT(idx >= mem_block_min_index(block) && idx <= mem_block_max_index(block),
                    "invalid index, idx: %lld, min idx: %lld, max idx: %lld",
                    idx,
                    mem_block_min_index(block),
                    mem_block_max_index(block));
            return block;
        }

        if (idx < block->start_index) {
            e = m - 1;
        } else {
            b = m + 1;
        }
    }

    INFQ_ERROR_LOG("block contains value indexed by '%lld' not found", idx);
    return NULL;
}
