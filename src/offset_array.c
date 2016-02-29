/**
 *
 * @file    offset_array
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 11:40:04
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "offset_array.h"
#include "infq.h"

#define DEFAULT_OFFSET_ARRAY_SIZE   1000

int32_t offset_array_expand(offset_array_t *offset_array);

int32_t
offset_array_init(offset_array_t *offset_array)
{
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    memset(offset_array, 0, sizeof(offset_array_t));

    offset_array->offsets = (int32_t *)malloc(sizeof(int32_t) * DEFAULT_OFFSET_ARRAY_SIZE);
    if (offset_array->offsets == NULL) {
        INFQ_ERROR_LOG("failed to alloc memory for offset array");
        return INFQ_ERR;
    }

    offset_array->capacity = DEFAULT_OFFSET_ARRAY_SIZE;

    return INFQ_OK;
}

int32_t
offset_array_expand(offset_array_t *offset_array){
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     *array;

    array = (int32_t *)realloc(
            offset_array->offsets,
            sizeof(int32_t) * offset_array->capacity * 2);
    if (array == NULL) {
        INFQ_ERROR_LOG("failed to expand offset array, err: %s", strerror(errno));
        return INFQ_ERR;
    }
    offset_array->offsets = array;
    offset_array->capacity *= 2;

    return INFQ_OK;
}

int32_t
offset_array_push(offset_array_t *offset_array, uint32_t offset)
{
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (offset_array->size >= offset_array->capacity) {
        if (offset_array_expand(offset_array) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to expand space, current capacity: %d",
                    offset_array->capacity);
            return INFQ_ERR;
        }
    }

    offset_array->offsets[offset_array->size++] = offset;

    return INFQ_OK;
}

int32_t
offset_array_cp(offset_array_t *dist, const offset_array_t *src)
{
    if (dist == NULL || src == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     size = offset_array_size(src);

    if (dist->capacity < size) {
        offset_array_destroy(dist);

        dist->offsets = (int32_t *)malloc(sizeof(int32_t) * size);
        if (dist->offsets == NULL) {
            INFQ_ERROR_LOG("failed to alloc mem for offset array");
            return INFQ_ERR;
        }
        dist->capacity = size;
    }

    memcpy(dist->offsets, src->offsets + src->start_idx, sizeof(int32_t) * size);
    dist->size = size;
    dist->start_idx = 0;

    return INFQ_OK;
}

int32_t
offset_array_get(const offset_array_t *offset_array, int32_t idx, int32_t *offset)
{
    if (offset_array == NULL || offset == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     actual_idx;

    actual_idx = idx + offset_array->start_idx;
    if (actual_idx >= offset_array->size) {
        INFQ_ERROR_LOG("invalid index, valid index: [0, %d]",
                offset_array->size - offset_array->start_idx);
        return INFQ_ERR;
    }

    *offset = offset_array->offsets[actual_idx];

    return INFQ_OK;
}

int32_t
offset_array_incr_start(offset_array_t *offset_array)
{
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (offset_array->start_idx >= offset_array->size) {
        INFQ_ERROR_LOG("failed to increase start index, must not larger than size, "
                "start: %d, size: %d",
                offset_array->start_idx,
                offset_array->size);
        return INFQ_ERR;
    }

    offset_array->offsets[offset_array->start_idx++] = INFQ_UNDEF;

    return INFQ_OK;
}

int32_t
offset_array_size(const offset_array_t *offset_array)
{
    return offset_array->size - offset_array->start_idx;
}

void
offset_array_destroy(offset_array_t *offset_array)
{
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    if (offset_array->offsets == NULL) {
        return;
    }

    free(offset_array->offsets);
    offset_array->offsets = NULL;
}

void
offset_array_reset(offset_array_t *offset_array)
{
    if (offset_array == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    offset_array->start_idx = 0;
    offset_array->size = 0;
}
