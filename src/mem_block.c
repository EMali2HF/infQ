/**
 *
 * @file    mem_block
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/10 12:04:47
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "mem_block.h"
#include "infq.h"
#include "sha1.h"

mem_block_t*
mem_block_init(int32_t block_size)
{
    mem_block_t *mem_block = (mem_block_t *)malloc(sizeof(mem_block_t) + block_size - 1);
    if (mem_block == NULL) {
        INFQ_ERROR_LOG("failed to alloc memory block");
        return NULL;
    }

    if (offset_array_init(&mem_block->offset_array) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to init offset array");
        goto failed;
    }

    mem_block->start_index = INFQ_UNDEF;
    mem_block->mem_size = block_size;
    mem_block->first_offset = 0;
    mem_block->last_offset = 0;
    mem_block->ele_count = 0;
    mem_block->file_block_no = INFQ_UNDEF;

    return mem_block;

failed:
    mem_block_destroy(mem_block);
    return NULL;
}

int32_t
mem_block_push(mem_block_t *mem_block, void *data, int32_t size)
{
    if (mem_block == NULL || data == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    // TODO: store a large object in two continuous memory blocks.
    if (mem_block_ele_size(size) > mem_block_avail(mem_block)) {
        INFQ_ERROR_LOG("no enough memory, data size: %d, mem available: %d",
                mem_block_ele_size(size),
                mem_block_avail(mem_block));
        return INFQ_ERR;
    }

    int32_t     *len_ptr, offset;

    // record offset index
    if (offset_array_push(&mem_block->offset_array, mem_block->last_offset) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to record offset info");
        return INFQ_ERR;
    }

    // write data len
    len_ptr = (int32_t *)(mem_block->mem + mem_block->last_offset);
    *len_ptr = size;
    mem_block->last_offset += sizeof(int32_t);

    // write data
    memcpy(mem_block->mem + mem_block->last_offset, data, size);
    mem_block->last_offset += size;

    // NOTICE: padding by 8 bytes
    offset = (mem_block->last_offset + 7) & (~INFQ_PADDING_MASK);
    if (offset <= mem_block->mem_size) {
        mem_block->last_offset = offset;
    }

    mem_block->ele_count++;

#ifdef D_ASSERT
    INFQ_ASSERT(mem_block->ele_count == offset_array_size(&mem_block->offset_array),
            "block and offset array aren't consistency, count: %d, offset size: %d"
            ", offset start: %lld, offset end: %d",
            mem_block->ele_count,
            offset_array_size(&mem_block->offset_array),
            mem_block->offset_array.start_idx,
            mem_block->offset_array.size);
#endif

    return INFQ_OK;
}

int32_t
mem_block_at(mem_block_t *mem_block, int64_t global_idx, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_block == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void    *dataptr;

    if (mem_block_at_zero_cp(mem_block, global_idx, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call at by zero copy");
        return INFQ_ERR;
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
mem_block_at_zero_cp(mem_block_t *mem_block, int64_t global_idx, const void **dataptr, int32_t *sizeptr)
{
    if (mem_block == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     local_idx;
    int32_t     offset;
    int32_t     *len_ptr;

#ifdef D_ASSERT
    INFQ_ASSERT(mem_block->ele_count == offset_array_size(&mem_block->offset_array),
            "block and offset array aren't consistency, count: %d, offset size: %d"
            "offset start: %d, offset end: %d",
            mem_block->ele_count,
            offset_array_size(&mem_block->offset_array),
            mem_block->offset_array.start_idx,
            mem_block->offset_array.size);
#endif

    local_idx = global_idx - mem_block->start_index;
    if (local_idx < 0 || local_idx >= mem_block->ele_count) {
        INFQ_ERROR_LOG("idx is invalid, index: %lld, valid index: [%lld, %lld]",
                global_idx,
                mem_block_min_index(mem_block),
                mem_block_max_index(mem_block));
        return INFQ_ERR;
    }

    if (offset_array_get(&mem_block->offset_array, local_idx, &offset) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to get offset by offset_array");
        return INFQ_ERR;
    }

    // make sure offset is valid
    if (offset < mem_block->first_offset || offset >= mem_block->last_offset) {
        INFQ_ERROR_LOG("invalid offset when call at, offset: %lld, invalid: [%lld, %lld)",
                offset,
                mem_block->first_offset,
                mem_block->last_offset);
        return INFQ_ERR;
    }

    len_ptr = (int32_t *)(mem_block->mem + offset);

    // check offset is valid, <= last offset
    if (offset + (int32_t)sizeof(int32_t) + *len_ptr >
            mem_block->last_offset) {
        INFQ_ERROR_LOG("data offset is invalid, beyong the boundary of block->last_offset, "
                "data offset: %d, last offset: %d, data size: %d",
                offset,
                mem_block->last_offset,
                *len_ptr);
        return INFQ_ERR;
    }

    *sizeptr = *len_ptr;
    *dataptr = mem_block->mem + offset + sizeof(int32_t);

    return INFQ_OK;
}

int32_t
mem_block_pop(mem_block_t *mem_block, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_block == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void    *data_buf;

    if (mem_block_pop_zero_cp(mem_block, &data_buf, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop element by zero copy");
        return INFQ_ERR;
    }

    if (*sizeptr == 0) {
        buf = NULL;
        return INFQ_OK;
    }

    if (buf_size < *sizeptr) {
        INFQ_ERROR_LOG("buffer is not enough, buf size: %d, expect: %d",
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    // copy data
    memcpy(buf, data_buf, *sizeptr);

    return INFQ_OK;
}

int32_t
mem_block_pop_zero_cp(mem_block_t *mem_block, const void **dataptr, int32_t *sizeptr)
{
    if (mem_block == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_INFO_LOG("invalid param");
        return INFQ_ERR;
    }

    // no more elements in the memory block
    if (mem_block_empty(mem_block)) {
        *dataptr = NULL;
        *sizeptr = 0;
        return INFQ_OK;
    }

    int32_t     *len_ptr;

    len_ptr = (int32_t *)(mem_block->mem + mem_block->first_offset);

    // check offset is valid, <= last offset
    if (mem_block->first_offset + (int32_t)sizeof(int32_t) + *len_ptr >
            mem_block->last_offset) {
        INFQ_ERROR_LOG("data offset is invalid, beyong the boundary of block->last_offset, "
                "first_offset: %d, last offset: %d, data size: %d",
                mem_block->first_offset,
                mem_block->last_offset,
                *len_ptr);
        return INFQ_ERR;
    }

    *sizeptr = *len_ptr;

    mem_block->first_offset += sizeof(int32_t);
    *dataptr = mem_block->mem + mem_block->first_offset;
    mem_block->first_offset += *sizeptr;

    // NOTICE: padding by 8 bytes
    mem_block->first_offset  = (mem_block->first_offset + 7) & (~INFQ_PADDING_MASK);

    mem_block->ele_count--;
    mem_block->start_index++;

    // NOTICE: update offset array, make sure that block and offset array are consistency
    if (offset_array_incr_start(&mem_block->offset_array) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to incr start index, ele count: %d, start index: %lld, fileno: %d",
                mem_block->ele_count,
                mem_block->start_index,
                mem_block->file_block_no);
        return INFQ_ERR;
    }

#ifdef D_ASSERT
    INFQ_ASSERT(mem_block->ele_count == offset_array_size(&mem_block->offset_array),
            "block and offset array aren't consistency, count: %d, offset size: %d"
            ", offset start: %d, offset end: %d",
            mem_block->ele_count,
            offset_array_size(&mem_block->offset_array),
            mem_block->offset_array.start_idx,
            mem_block->offset_array.size);
#endif

    return INFQ_OK;
}

int32_t
mem_block_just_pop(mem_block_t *mem_block)
{
    if (mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void      *dataptr;
    int32_t         size;

    if (mem_block_pop_zero_cp(mem_block, &dataptr, &size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop by zero copy");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
mem_block_top(mem_block_t *mem_block, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (mem_block == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void        *dataptr;

    if (mem_block_top_zero_cp(mem_block, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to get top element by zero copy");
        *sizeptr = 0;
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

    // copy data
    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
mem_block_top_zero_cp(mem_block_t *mem_block, const void **dataptr, int32_t *sizeptr)
{
    if (mem_block == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (mem_block_empty(mem_block)) {
        *dataptr = NULL;
        *sizeptr = 0;
        return INFQ_OK;
    }

    int32_t     *len_ptr;

    len_ptr = (int32_t *)(mem_block->mem + mem_block->first_offset);

    // check offset is valid, <= last offset
    if (mem_block->first_offset + (int32_t)sizeof(int32_t) + *len_ptr >
            mem_block->last_offset) {
        INFQ_ERROR_LOG("data offset is invalid, beyong the boundary of block->last_offset, "
                "first_offset: %d, last offset: %d, data size: %d",
                mem_block->first_offset,
                mem_block->last_offset,
                *len_ptr);
        return INFQ_ERR;
    }

    *sizeptr = *len_ptr;
    *dataptr = mem_block->mem + mem_block->first_offset + sizeof(int32_t);

    return INFQ_OK;
}

int32_t
mem_block_reset(mem_block_t *mem_block, int64_t start_index)
{
    if (mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    mem_block->start_index = start_index;
    mem_block->first_offset = mem_block->last_offset = 0;
    mem_block->ele_count = 0;
    mem_block->file_block_no = INFQ_UNDEF;

    offset_array_reset(&mem_block->offset_array);

    return INFQ_OK;
}

void
mem_block_destroy(mem_block_t *mem_block)
{
    if (mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    offset_array_destroy(&mem_block->offset_array);
    free(mem_block);
}

int32_t
mem_block_debug_info(mem_block_t *mem_block, char *buf, int32_t size)
{
    if (buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     ret;

    if (mem_block == NULL || mem_block->start_index == INFQ_UNDEF) {
        buf[0] = '\0';
    } else {
        ret = snprintf(buf, size,
                "      start_index: %lld,\r\n"
                "      ele_count: %d,\r\n"
                "      first_offset: %d,\r\n"
                "      last_offset: %d,\r\n"
                "      file_block_no: %d,\r\n",
                mem_block->start_index,
                mem_block->ele_count,
                mem_block->first_offset,
                mem_block->last_offset,
                mem_block->file_block_no);
        if (ret == -1 || ret >= size) {
            INFQ_ERROR_LOG("buf is too small or error");
            return INFQ_ERR;
        }
    }

    return INFQ_OK;
}

int32_t
mem_block_signature(mem_block_t *mem_block, unsigned char digest[20])
{
    if (mem_block == NULL || digest == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char        buf[INFQ_MAX_BUF_SIZE];
    int         ret;
    SHA1_CTX    ctx;

    ret = snprintf(buf, INFQ_MAX_BUF_SIZE, "si=%lld;fo=%d;lo=%d;ec=%d",
            mem_block->start_index,
            mem_block->first_offset,
            mem_block->last_offset,
            mem_block->ele_count);
    if (ret < 0) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to format signature string");
        return INFQ_ERR;
    } else if (ret >= INFQ_MAX_BUF_SIZE) {
        INFQ_ERROR_LOG("failed to format signature string, buffer is too small, "
                "buf size: %d, expected: %d",
                INFQ_MAX_BUF_SIZE,
                ret);
        return INFQ_ERR;
    }

    SHA1Init(&ctx);
    SHA1Update(&ctx, (unsigned char *)buf, ret);
    SHA1Final(digest, &ctx);

    return INFQ_OK;
}
