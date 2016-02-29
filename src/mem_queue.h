/**
 *
 * @file    mem_queue
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/10 18:58:52
 */

#ifndef COM_MOMO_INFQ_MEM_QUEUE_H
#define COM_MOMO_INFQ_MEM_QUEUE_H

#include "mem_block.h"

#define first_block(q)              (q)->blocks[(q)->first_block]
#define last_block(q)               (q)->blocks[(q)->last_block]
#define mem_queue_full(q)           ((q)->first_block == ((q)->last_block + 1) % (q)->block_num)
#define mem_queue_has_full_block(q) ((q)->first_block != (q)->last_block)

// the last block is not used
#define mem_queue_full_block_num(q) ((q)->last_block - (q)->first_block + (q)->block_num) % (q)->block_num
#define mem_queue_free_block_num(q) (q)->block_num - 1 - mem_queue_full_block_num(q)
#define mem_queue_empty(q)          ((q)->first_block == (q)->last_block && \
        mem_block_empty(first_block( (q) )))

// add callback funtion and argument
#define mem_queue_add_pop_blk_cb(q,cb,arg)  do { (q)->pop_blk_cb = (cb); (q)->pop_blk_cb_arg = (arg); } while(0)
#define mem_queue_add_push_blk_cb(q,cb,arg)  do { (q)->push_blk_cb = (cb); (q)->push_blk_cb_arg = (arg); } while(0)

typedef int32_t (*mem_queue_pop_blk_cb_t)(void *arg, mem_block_t *blk);
typedef int32_t (*mem_queue_push_blk_cb_t)(void *arg);

typedef struct _mem_queue_t {
    // circle queue
    mem_block_t             **blocks;
    int32_t                 block_num;
    volatile int32_t        first_block;
    volatile int32_t        last_block;
    volatile int32_t        ele_count;
    // [min_idx, max_idx) is the index range of the queue
    // min_idx == max_idx means that the queue is empty
    volatile int64_t        min_idx;
    volatile int64_t        max_idx;
    mem_queue_pop_blk_cb_t  pop_blk_cb;
    void                    *pop_blk_cb_arg;
    mem_queue_push_blk_cb_t push_blk_cb;
    void                    *push_blk_cb_arg;
} mem_queue_t;

int32_t mem_queue_init(mem_queue_t *mem_queue, int32_t block_num, int32_t block_size);
int32_t mem_queue_push(mem_queue_t *mem_queue, int64_t ele_idx, void *data, int32_t size);
int32_t mem_queue_pop(mem_queue_t *mem_queue, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_queue_just_pop(mem_queue_t *mem_queue);
int32_t mem_queue_top(mem_queue_t *mem_queue, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_queue_at(mem_queue_t *mem_queue, int64_t idx, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t mem_queue_jump(mem_queue_t *mem_queue); //only used by push queue
void mem_queue_destroy(mem_queue_t *mem_queue);
void mem_queue_reset(mem_queue_t *mem_queue);

int32_t mem_queue_pop_zero_cp(mem_queue_t *mem_queue, const void **dataptr, int32_t *sizeptr);
int32_t mem_queue_top_zero_cp(mem_queue_t *mem_queue, const void **dataptr, int32_t *sizeptr);
int32_t mem_queue_at_zero_cp(mem_queue_t *mem_queue, int64_t idx, const void **dataptr, int32_t *sizeptr);

#endif
