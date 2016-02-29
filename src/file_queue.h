/**
 *
 * @file    file_queue
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 11:32:08
 */

#ifndef COM_MOMO_INFQ_FILE_QUEUE_H
#define COM_MOMO_INFQ_FILE_QUEUE_H

#include <stdint.h>
#include <pthread.h>

#include "file_block.h"
#include "file_block_index.h"
#include "mem_block.h"

typedef struct _file_queue_t {
    file_block_t        *block_head, *block_tail;   /* All the blocks in a file queue are organized into
                                                       a linked-list. 'block_head' and 'block_tail' point
                                                       to the head and tail of the linked-list respectively */
    int32_t             block_suffix;               /* Suffix of the file block which will be appended soon */
    volatile int32_t    block_num;                  /* Number of the blocks in the file queue */
    volatile int32_t    ele_count;                  /* Element count */
    volatile int32_t    total_fsize;                /* Total file size of the file queue */
    file_block_index_t  index;                      /* An index used to search a file block by global index */
    char                *file_path;                 /* File path used to store files of InfQ */
    pthread_mutex_t     mu;
} file_queue_t;

int32_t file_queue_init(file_queue_t *file_queue, const char *data_path);
int32_t file_queue_at(
        file_queue_t *file_queue,
        int64_t global_idx,
        void *buf,
        int32_t buf_size,
        int32_t *size);
int32_t file_queue_dump_block(file_queue_t *file_queue, mem_block_t *mem_block);
int32_t file_queue_load_block(file_queue_t *file_queue, mem_block_t *mem_block);

/**
 * @brief Load file blocks on the disk by the suffix of the file name, which is used
 *      to load the whole infQ. A dumped file queue is represented by a series of
 *      files suffixed by continuous number.
 * @param file_suffix: Suffix of the file name.
 */
int32_t file_queue_add_block_by_file(file_queue_t *file_queue, int32_t file_suffix);
void file_queue_destroy(file_queue_t *file_queue);

/**
 * @brief Destroy the file queue and remove all files belong to the queue
 */
int32_t file_queue_destroy_completely(file_queue_t *file_queue);

int32_t file_queue_empty(file_queue_t *file_queue);

#endif
