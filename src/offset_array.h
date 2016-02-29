/**
 *
 * store the offset of the element at index in memory block or file block.
 *
 * @file    offset_array
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 11:36:59
 */

#ifndef COM_MOMO_INFQ_OFFSET_ARRAY_H
#define COM_MOMO_INFQ_OFFSET_ARRAY_H

#include <stdint.h>

#define offset_array_msize(a)       (int32_t)(sizeof(int32_t) * (a)->capacity)

typedef struct _offset_array_t {
    int32_t     *offsets;
    // [start_idx, size) are the valid offsets
    int32_t     start_idx;
    int32_t     size;
    int32_t     capacity;
} offset_array_t;

int32_t offset_array_init(offset_array_t *offset_array);
int32_t offset_array_push(offset_array_t *offset_array, uint32_t offset);
int32_t offset_array_cp(offset_array_t *dist, const offset_array_t *src);

/**
 * idx is the logical index, it need to be converted to actual index
 * actual index = offset_array->start_idx + idx;
 */
int32_t offset_array_get(const offset_array_t *offset_array, int32_t idx, int32_t *offset);
int32_t offset_array_incr_start(offset_array_t *offset_array);
int32_t offset_array_size(const offset_array_t *offset_array);
void offset_array_destroy(offset_array_t *offset_array);
void offset_array_reset(offset_array_t *offset_array);

#endif
