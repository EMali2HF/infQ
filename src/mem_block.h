/**
 *
 * @file    mem_block
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/10 11:54:24
 */

#ifndef COM_MOMO_INFQ_MEM_BLOCK_H
#define COM_MOMO_INFQ_MEM_BLOCK_H

#include <stdint.h>

#include "offset_array.h"

#define mem_block_ele_size(size)    (int32_t)(size + sizeof(int32_t))
#define mem_block_avail(blk)        (blk)->mem_size - (blk)->last_offset
#define mem_block_empty(blk)        ((blk)->first_offset >= (blk)->last_offset)
#define mem_block_min_index(blk)    (blk)->start_index
#define mem_block_max_index(blk)    ((blk)->start_index + (blk)->ele_count - 1)
#define mem_block_file_blk_no(blk)  (blk)->file_block_no

#define INFQ_PADDING_MASK   0x07

typedef struct _mem_block_t {
    volatile int64_t    start_index;    /* Global index of the first element in the block */
    int32_t             mem_size;       /* Size of the block */
    int32_t             first_offset;   /* Offset of the first element */
    int32_t             last_offset;    /* 'last_offset - 1' is the offset of the end of the last element.
                                           It points to the beginning of the next element */
    volatile int32_t    ele_count;      /* Elements count */
    int32_t             file_block_no;  /* When memory block is loaded from a file block, 'file_block_no'
                                           specifies the file descriptor of the file block */
    offset_array_t      offset_array;   /* Mapping the offset of element by index */
    char                mem[1];
} mem_block_t;

mem_block_t* mem_block_init(int32_t block_size);
int32_t mem_block_push(mem_block_t *mem_block, void *data, int32_t size);
int32_t mem_block_pop(mem_block_t *mem_block, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_block_just_pop(mem_block_t *mem_block);
int32_t mem_block_at(mem_block_t *mem_block, int64_t global_idx, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_block_top(mem_block_t *mem_block, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_block_reset(mem_block_t *mem_block, int64_t start_index);

/**
 * Fetch the signature of the memory block. Used to check the consistency of
 *      memory block and file block.
 */
int32_t mem_block_signature(mem_block_t *mem_block, unsigned char digest[20]);
void mem_block_destroy(mem_block_t *mem_block);

int32_t mem_block_pop_zero_cp(mem_block_t *mem_block, const void **dataptr, int32_t *sizeptr);
int32_t mem_block_at_zero_cp(mem_block_t *mem_block, int64_t global_idx, const void **dataptr, int32_t *sizeptr);
int32_t mem_block_top_zero_cp(mem_block_t *mem_block, const void **dataptr, int32_t *sizeptr);
int32_t mem_block_debug_info(mem_block_t *mem_block, char *buf, int32_t size);

#endif
