/**
 *
 * @file    infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 10:53:12
 */

#ifndef COM_MOMO_INFQ_INFQ_H
#define COM_MOMO_INFQ_INFQ_H

#include <stdint.h>

#include "logging.h"

#define INFQ_OK     0
#define INFQ_ERR    -1
#define INFQ_UNDEF  -1
#define INFQ_TRUE   1
#define INFQ_FALSE  0

#define INFQ_MAGIC_NUMBER   "INFQUEUE"
#define INFQ_MAX_BUF_SIZE   1024
#define INFQ_MAX_PATH_SIZE  100

#define INFQ_DUMP_BG_EXEC       1
#define INFQ_LOAD_BG_EXEC       2
#define INFQ_UNLINK_BG_EXEC     3

typedef struct _infq_t infq_t;

typedef struct _infq_config_t {
    const char  *data_path;             /* File path to store files of InfQ */
    int32_t     mem_block_size;         /* The size of memory block */
    int32_t     pushq_blocks_num;       /* Number of memory blocks in push queue */
    int32_t     popq_blocks_num;        /* Number of memory blocks in pop queue*/
    float       block_usage_to_dump;    /* When the percentage of used memory blocks in push queue
                                           is greater than this value, 'Dumper' will try to dump the
                                           blocks in push queue */
} infq_config_t;

typedef struct _file_suffix_range {
    // [start, end)
    int32_t     start;
    int32_t     end;
} file_suffix_range;

typedef struct _popq_dump_meta_t {
    // valid suffix range of pop block files is [blk_start, blk_end)
    file_suffix_range   file_range;
    int64_t             min_idx;
    int64_t             max_idx;
    int32_t             ele_count;
    int32_t             block_num;
    int32_t             block_size;
} popq_dump_meta_t;

typedef struct _file_dump_meta_t {
    // valid suffix range of file block files is [blk_start, blk_end)
    file_suffix_range   file_range;
    int32_t             ele_count;
    int32_t             block_num;
    int32_t             file_size;
} file_dump_meta_t;

typedef struct _infq_dump_meta_t {
    const char          *file_path;
    const char          *infq_name;
    int32_t             file_path_len;
    int32_t             infq_name_len;
    int64_t             global_ele_idx;
    // file queue meta
    file_dump_meta_t    file_meta;
    // pop queue meta
    popq_dump_meta_t    popq_meta;
} infq_dump_meta_t;

typedef struct _infq_bg_exec_stats_t {
    int32_t     is_suspended;
    int32_t     job_num;
} infq_bg_exec_stats_t;

typedef struct _infq_stats_t {
    int32_t                 mem_size;
    int32_t                 file_size;
    int32_t                 mem_block_size;
    int32_t                 pushq_blocks_num;
    int32_t                 pushq_used_blocks;
    int32_t                 popq_blocks_num;
    int32_t                 popq_used_blocks;
    int32_t                 fileq_blocks_num;
    infq_bg_exec_stats_t    dumper;
    infq_bg_exec_stats_t    loader;
    infq_bg_exec_stats_t    unlinker;
} infq_stats_t;

extern char *INFQ_VERSION;
extern char *INFQ_FILE_BLOCK_PREFIX;
extern char *INFQ_POP_BLOCK_PREFIX;

infq_t* infq_init(const char *data_path, const char *name);
infq_t* infq_init_by_conf(const infq_config_t *conf, const char *name);
int32_t infq_push(infq_t *infq, void *data, int32_t size);
int32_t infq_pop(infq_t *infq, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t infq_at(infq_t *infq, int64_t idx, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t infq_top(infq_t *infq, void *buf, int32_t buf_size, int32_t *sizeptr);
int32_t infq_just_pop(infq_t *infq);
int32_t infq_size(infq_t *infq);
int32_t infq_msize(infq_t *infq);
int32_t infq_fsize(infq_t *infq);
void infq_destroy(infq_t *infq);

/**
 * @brief Destroy InfQ and remove all the files belong to InfQ.
 */
int32_t infq_destroy_completely(infq_t *infq);

int32_t infq_pop_zero_cp(infq_t *infq, const void **dataptr, int32_t *sizeptr);
int32_t infq_top_zero_cp(infq_t *infq, const void **dataptr, int32_t *sizeptr);
int32_t infq_at_zero_cp(infq_t *infq, int64_t idx, const void **dataptr, int32_t *sizeptr);

int32_t infq_check_pushq(infq_t *infq);
int32_t infq_check_popq(infq_t *infq);

int32_t infq_suspend_bg_exec(infq_t *infq, int32_t exec_type);
int32_t infq_continue_bg_exec(infq_t *infq, int32_t exec_type);
int32_t infq_continue_bg_exec_if_suspended(infq_t *infq, int32_t exec_type);

/**
 * @brief Make the push queue jump to use next memory block, a special
 *      function to make sure a consistent file-queue block file even it
 *      written twice by background dumper and infq dump.
 */
int32_t infq_push_queue_jump(infq_t *infq);

/**
 * @brief Dump the infQ to a buffer. A series of file may be created, which represent
 *      push queue and pop queue.
 * @param buf: buffer used to store the dumped data.
 * @param buf_size: the size of the buffer.
 * @param data_size: the size of the dumped data.
 */
int32_t infq_dump(infq_t *infq, char *buf, int32_t buf_size, int32_t *data_size);

/**
 * @brief Load the infQ from a buffer. All the files which are belonged to file queue
 *      and pop queue will be opend and read, so this is a slow operation.
 *      Normally, file queue won't be used, noly some pop blocks need to be read.
 */
int32_t infq_load(infq_t *infq, const char *buf, int32_t buf_size);

/**
 * @brief Fetch all the persistent meta information which is used to replicate.
 */
infq_dump_meta_t* infq_fetch_dump_meta(infq_t *infq);

int32_t infq_fetch_stats(infq_t *infq, infq_stats_t *stats);

const char* infq_debug_info(infq_t *infq, char *buf, int32_t size);

/**
 * @brief 用于切换dump meta，并将需要删除的文件添加到unlinker队列
 *      需要删除的文件是两次dump的，文件版本diff
 */
int32_t infq_done_dump(infq_t *infq);

#endif
