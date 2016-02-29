/**
 *
 * @file    infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 10:59:19
 */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/mman.h>

#include "infq.h"
#include "mem_queue.h"
#include "file_queue.h"
#include "file_block.h"
#include "bg_job.h"
#include "infq_bg_jobs.h"
#include "utils.h"

#define INFQ_DEFAULT_MEM_BLOCK_USAGE    0.5
#define INFQ_CHECK_LOAD_PER_CALLS       50
#define INFQ_NO_RETURN                  1
#define INFQ_DUMP_META_LEN              16
#define INFQ_NAME_MAX_LEN               100

#define dump_meta_len(meta)             (INFQ_DUMP_META_LEN + sizeof(infq_dump_meta_t) + \
        (meta)->file_path_len + (meta)->infq_name_len)
#define cur_dump_meta(infq)             ((infq)->dump_meta_double_buf[infq->cur_meta_idx])
#define backup_dump_meta(infq)          ((infq)->dump_meta_double_buf[1 - infq->cur_meta_idx])

char *INFQ_VERSION = "v0.1.0";
char *INFQ_POP_BLOCK_PREFIX = "pop_block";

infq_config_t default_conf = {
    NULL,
    10 * 1024 * 1024,
    20,
    20,
    0.5
};

struct _infq_t {
    int64_t             global_ele_idx;         /* Index of the next element pushed */
    mem_queue_t         push_queue, pop_queue;
    file_queue_t        file_queue;
    bg_exec_t           dump_exec, load_exec, unlink_exec; /* Background executor of dumper, loader
                                                              and unlinker */
    pthread_mutex_t     push_mu, pop_mu;        /* Mutexes for push queue and pop queue */
    mem_block_t         *tmp_mem_block;         /* A temporary memory block used to load file block */
    int32_t             mem_block_size;         /* The size of memory block  */
    infq_dump_meta_t    *dump_meta_double_buf;  /* Persistent status of a dumped InfQ.
                                                   - Double buffer is used to ensure consistency, and the
                                                     current one is specified by 'cur_meta_idx'.
                                                   - For the comunication between parent and child processes
                                                     when redis is saving background(RDB), shared memory is
                                                     used. */
    int32_t             cur_meta_idx;           /* Index of current dump meta */
    int32_t             pop_block_suffix;       /* The suffix of the next pop block when dumping */
    float               block_usage_to_dump;    /* When the percentage of used memory blocks in push queue
                                                   is greater than this value, 'Dumper' will try to dump the
                                                   blocks in push queue */
    char                name[INFQ_NAME_MAX_LEN];    /* Name of the InfQ */
};

/* Functions for background executors */
int32_t dump_job(void *);
int32_t load_job(void *);

void swap_mem_block(infq_t *infq);
int32_t check_and_trigger_loader(infq_t *infq);
int32_t dump_push_queue(infq_t *infq);
int32_t dump_pop_queue_if_need(infq_t *infq, popq_dump_meta_t *meta);
int32_t load_file_queue(infq_t *infq, infq_dump_meta_t *meta);
int32_t load_pop_queue(infq_t *infq, infq_dump_meta_t *meta);
const char* infq_debug_info(infq_t *infq, char *buf, int32_t size);

int32_t empty_block_pop_callback(void *arg, mem_block_t *blk);
int32_t full_block_push_callback(void *arg);

infq_t*
infq_init_by_conf(const infq_config_t *conf, const char *name)
{
    if (conf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return NULL;
    }

    infq_t                  *infq;
    int32_t                 err;
    pthread_mutexattr_t     mu_attr;

    if (strlen(name) + 1 > INFQ_NAME_MAX_LEN) {
        INFQ_ERROR_LOG("name is too long, %d chars at most, name: %s",
                INFQ_NAME_MAX_LEN,
                name);
        return NULL;
    }

    if (strlen(conf->data_path) > INFQ_MAX_PATH_SIZE - 1) {
        INFQ_ERROR_LOG("data path is too long, %d chars at most, path: %s",
                INFQ_MAX_PATH_SIZE,
                conf->data_path);
        return NULL;
    }

    infq = (infq_t *)malloc(sizeof(infq_t));
    if (infq == NULL) {
        INFQ_ERROR_LOG("[%s]failed to alloc memory for infq", name);
        return NULL;
    }

    memset(infq, 0, sizeof(infq_t));
    strcpy(infq->name, name);

    // Allocate shared memory for demp meta data
    infq->dump_meta_double_buf = mmap(
            NULL,
            sizeof(infq_dump_meta_t) * 2,
            PROT_READ | PROT_WRITE,
            MAP_ANON | MAP_SHARED,
            -1,
            0);
    if (infq->dump_meta_double_buf == NULL) {
        INFQ_ERROR_LOG_BY_ERRNO("[%s]failed to alloc shared mem", infq->name);
        goto failed;
    }
    memset(infq->dump_meta_double_buf, 0, sizeof(infq_dump_meta_t) * 2);

    // Create directory for InfQ
    if (make_sure_data_path(conf->data_path) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to make sure data path, path: %s", conf->data_path);
        goto failed;
    }

    // init push queue and pop queue
    if (mem_queue_init(&infq->push_queue, conf->pushq_blocks_num, conf->mem_block_size)
            == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init push queue", name);
        goto failed;
    }
    mem_queue_add_push_blk_cb(&infq->push_queue, full_block_push_callback, infq);

    if (mem_queue_init(&infq->pop_queue, conf->popq_blocks_num, conf->mem_block_size)
            == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init pop queue", name);
        goto failed;
    }
    mem_queue_add_pop_blk_cb(&infq->pop_queue, empty_block_pop_callback, infq);

    if (file_queue_init(&infq->file_queue, conf->data_path) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init file queue", name);
        goto failed;
    }

    // init background executor
    if (bg_exec_init(&infq->dump_exec, "dumper") == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init dumper", name);
        goto failed;
    }

    if (bg_exec_init(&infq->load_exec, "loader") == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init loader", name);
        goto failed;
    }

    if (bg_exec_init(&infq->unlink_exec, "unlinker") == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init unlinker", name);
        goto failed;
    }

    // init and set type of mutex attr
    if (pthread_mutexattr_init(&mu_attr) != 0) {
        INFQ_ERROR_LOG("[%s]failed to init mutex attr", name);
        goto failed;
    }

    if (pthread_mutexattr_settype(&mu_attr, PTHREAD_MUTEX_ERRORCHECK) != 0) {
        INFQ_ERROR_LOG("failed to set type of mutex attr");
        goto failed;
    }

    if (pthread_mutex_init(&infq->push_mu, &mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to init mutex of push queue");
        goto failed;
    }

    if (pthread_mutex_init(&infq->pop_mu, &mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to init mutex of pop queue");
        goto failed;
    }

    if (pthread_mutexattr_destroy(&mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to destory mutex attr");
        goto failed;
    }

    infq->tmp_mem_block = mem_block_init(conf->mem_block_size);
    if (infq->tmp_mem_block == NULL) {
        INFQ_ERROR_LOG("[%s]failed to init temp mem block", name);
        goto failed;
    }
    infq->mem_block_size = conf->mem_block_size;
    infq->block_usage_to_dump = conf->block_usage_to_dump;

    INFQ_DEBUG_LOG("[%s]successful to init InfQ, mem block size: %d,"
            "pushq blocks: %d, popq blocks: %d, block usage: %f, meta_idx: %d",
            name,
            conf->mem_block_size,
            conf->pushq_blocks_num,
            conf->popq_blocks_num,
            conf->block_usage_to_dump,
            infq->cur_meta_idx);

    return infq;

failed:
    if ((err = pthread_mutex_destroy(&infq->push_mu)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destroy push mu");
    }

    if ((err = pthread_mutex_destroy(&infq->pop_mu)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destroy pop mu");
    }

    if ((err = pthread_mutexattr_destroy(&mu_attr)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destroy mutex attr");
    }

    infq_destroy(infq);

    return NULL;
}

infq_t*
infq_init(const char *data_path, const char *name)
{
    if (data_path == NULL) {
        return NULL;
    }
    default_conf.data_path = data_path;

    return infq_init_by_conf(&default_conf, name);
}

int32_t
infq_push(infq_t *infq, void *data, int32_t size)
{
    if (infq == NULL || data == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int     ret;

    ret = INFQ_OK;
    infq_pthread_mutex_lock(&infq->push_mu);
    do {
        if (mem_queue_full(&infq->push_queue)) {
            INFQ_DEBUG_LOG("[%s]push queue is full, block idx: [%d, %d], index: [%d, %d], "
                    "dumper jobs: %d",
                    infq->name,
                    infq->push_queue.first_block,
                    infq->push_queue.last_block,
                    infq->push_queue.min_idx,
                    infq->push_queue.max_idx,
                    bg_exec_pending_task_num(&infq->dump_exec));
            ret = INFQ_ERR;
            break;
        }

        if (mem_queue_push(&infq->push_queue, infq->global_ele_idx, data, size) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to push data to push queue, index: %lld",
                    infq->name,
                    infq->global_ele_idx);
            ret = INFQ_ERR;
            break;
        }
        infq->global_ele_idx++;
    } while(0);
    infq_pthread_mutex_unlock(&infq->push_mu);

    return ret;
}

int32_t
infq_pop_zero_cp(infq_t *infq, const void **dataptr, int32_t *sizeptr)
{
    if (infq == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     ret;

    // 1. try to pop from pop queue
    infq_pthread_mutex_lock(&infq->pop_mu);
    if (!mem_queue_empty(&infq->pop_queue)) {
        if (mem_queue_pop_zero_cp(&infq->pop_queue, dataptr, sizeptr) == INFQ_ERR) {
            infq_pthread_mutex_unlock(&infq->pop_mu);
            INFQ_ERROR_LOG("[%s]failed to pop from pop queue", infq->name);
            return INFQ_ERR;
        }
        infq_pthread_mutex_unlock(&infq->pop_mu);

        return INFQ_OK;
    }
    infq_pthread_mutex_unlock(&infq->pop_mu);

    // 2. try to pop from push queue
    pthread_mutex_lock(&infq->file_queue.mu);
    pthread_mutex_lock(&infq->push_mu);
    do {
        ret = INFQ_ERR;
        // file queue is empty
        if (infq->file_queue.block_num == 0) {
            // push queue is empty means the infQ is empty
            if (mem_queue_empty(&infq->push_queue)) {
                INFQ_DEBUG_LOG("[%s]queue is empty", infq->name);
                *dataptr = NULL;
                *sizeptr = 0;
                ret = INFQ_OK;
                break;
            }

            // try to pop from push queue when file queue is empty
            if (mem_queue_pop_zero_cp(&infq->push_queue, dataptr, sizeptr) == INFQ_ERR) {
                INFQ_ERROR_LOG("[%s]failed to pop from push queue", infq->name);
                break;
            }

            // NOTICE: Poping data from push queue means that the pop queue is empty.
            //      Update the index range of pop queue to make sure that the index
            //      ranges of pop queue and push queue are continuous.
            //      更新pop queue的index的逻辑必须在从push queue pop之后，否则push queue
            //      和pop queue的index不连续，并且pop queue的min_idx与其第一个block的start_idx
            //      不一致，popq->min_idx + 1 = first_block(popq)->start_idx
            infq->pop_queue.min_idx = infq->push_queue.min_idx;
            infq->pop_queue.max_idx = infq->push_queue.min_idx;

            ret = INFQ_OK;
        } else {
            // NOTICE: just return error when data in file queue is not loaded to memory.
            INFQ_ERROR_LOG("[%s]data is in file queue, need to load to memory queue",
                    infq->name);
        }
    } while (0);

    // NOTICE: 解锁顺序不同是否会死锁？
    pthread_mutex_unlock(&infq->file_queue.mu);
    pthread_mutex_unlock(&infq->push_mu);

    return ret;
}

int32_t
infq_pop(infq_t *infq, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (infq == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void  *dataptr;

    if (infq_pop_zero_cp(infq, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to pop zero copy", infq->name);
        return INFQ_ERR;
    }

    if (*sizeptr > buf_size) {
        INFQ_ERROR_LOG("[%s]buffer is not enough, buf size: %d, expect: %d",
                infq->name,
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
infq_at(infq_t *infq, int64_t idx, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (infq == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     ret;

    // convert to absolute index, n <= [0, ele_count] => m <= [min_idx, max_idx]
    if (infq->pop_queue.min_idx == INFQ_UNDEF) {
        // pop queue hasn't been used, all data is in push queue
        idx += infq->push_queue.min_idx;
    } else {
        idx += infq->pop_queue.min_idx;
    }

    if (idx < infq->pop_queue.min_idx || idx >= infq->push_queue.max_idx) {
        INFQ_ERROR_LOG("[%s]invalid index when qat, valid index range: [%lld, %lld)",
                infq->name,
                infq->pop_queue.min_idx,
                infq->push_queue.max_idx);
        return INFQ_ERR;
    }

    // in push queue, pushq.min_idx <= idx < pushq.max_idx
    ret = INFQ_NO_RETURN;
    infq_pthread_mutex_lock(&infq->push_mu);
    if (idx >= infq->push_queue.min_idx) {
        if (mem_queue_at(&infq->push_queue, idx, buf, buf_size, sizeptr) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to fetch by index from push queue, idx: %lld",
                    infq->name,
                    idx);
            ret = INFQ_ERR;
        } else {
            ret = INFQ_OK;
        }
    }
    infq_pthread_mutex_unlock(&infq->push_mu);

    if (ret == INFQ_OK || ret == INFQ_ERR) {
        return ret;
    }

    // in pop queue, popq.min_idx <= idx < popq.max_idx
    ret = INFQ_NO_RETURN;
    infq_pthread_mutex_lock(&infq->pop_mu);
    if (idx < infq->pop_queue.max_idx) {
        if (mem_queue_at(&infq->pop_queue, idx, buf, buf_size, sizeptr) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to fetch by index from pop queue, idx: %lld",
                    infq->name,
                    idx);
            ret = INFQ_ERR;
        } else {
            ret = INFQ_OK;
        }
    }
    infq_pthread_mutex_unlock(&infq->pop_mu);

    if (ret == INFQ_OK || ret == INFQ_ERR) {
        return ret;
    }

    // in file queue, popq.max_idx <= idx <  pushq.min_idx
    if (file_queue_at(&infq->file_queue, idx, buf, buf_size, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to fetch by index from file queue, idx: %lld",
                infq->name,
                idx);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_at_zero_cp(infq_t *infq, int64_t idx, const void ** dataptr, int32_t *sizeptr)
{
    if (infq == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int     ret;

    // convert to absolute index, n <= [0, ele_count] => m <= [min_idx, max_idx]
    if (infq->pop_queue.min_idx == INFQ_UNDEF) {
        idx += infq->push_queue.min_idx;
    } else {
        idx += infq->pop_queue.min_idx;
    }

    if (idx >= infq->push_queue.max_idx || idx < infq->pop_queue.min_idx) {
        INFQ_ERROR_LOG("[%s]invalid index, valid index range: [%lld, %lld)",
                infq->name,
                infq->pop_queue.min_idx,
                infq->push_queue.max_idx);
        return INFQ_ERR;
    }

    // in file queue, forbidden by zero copy
    if (idx < infq->push_queue.min_idx && idx >= infq->pop_queue.max_idx) {
        INFQ_ERROR_LOG("[%s]fetch by index in zero copy mode is not allowed in file queue,"
                "idx: %lld, file queue: [%lld, %lld)",
                infq->name,
                idx,
                infq->pop_queue.max_idx,
                infq->push_queue.min_idx);
        return INFQ_ERR;
    }

    ret = INFQ_NO_RETURN;
    // in push queue
    infq_pthread_mutex_lock(&infq->push_mu);
    if (idx >= infq->push_queue.min_idx) {
        if (mem_queue_at_zero_cp(&infq->push_queue, idx, dataptr, sizeptr) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to fetch by index in zero copy mode from push queue",
                    infq->name);
            ret = INFQ_ERR;
        } else {
            ret = INFQ_OK;
        }
    }
    infq_pthread_mutex_unlock(&infq->push_mu);

    if (ret != INFQ_NO_RETURN) {
        return ret;
    }

    // in pop queue
    infq_pthread_mutex_lock(&infq->pop_mu);
    if (mem_queue_at_zero_cp(&infq->pop_queue, idx, dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to fetch by index in zero copy mode from pop queue",
                infq->name);
        ret = INFQ_ERR;
    } else {
        ret = INFQ_OK;
    }
    infq_pthread_mutex_unlock(&infq->pop_mu);

    return ret;
}

int32_t
infq_top_zero_cp(infq_t *infq, const void **dataptr, int32_t *sizeptr)
{
    if (infq == NULL || dataptr == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     popq_empty;

    // 1. top in pop queue when pop queue is not empty
    infq_pthread_mutex_lock(&infq->pop_mu);
    popq_empty = mem_queue_empty(&infq->pop_queue);
    infq_pthread_mutex_unlock(&infq->pop_mu);
    if (!popq_empty) {
        if (mem_queue_top_zero_cp(&infq->pop_queue, dataptr, sizeptr) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to fetch top from pop queue in zero mode", infq->name);
            return INFQ_ERR;
        }

        return INFQ_OK;
    }

    // 2. just return when pop queue is empty and file queue isn't empty
    if (!file_queue_empty(&infq->file_queue)) {
        INFQ_ERROR_LOG("[%s]faile to fetch top, file queue is not empty, need to load to memory",
                infq->name);
        return INFQ_ERR;
    }

    // 3. top in push queue when pop queue and file queue are both empty
    infq_pthread_mutex_lock(&infq->push_mu);
    if (mem_queue_top_zero_cp(&infq->push_queue, dataptr, sizeptr) == INFQ_ERR) {
        infq_pthread_mutex_unlock(&infq->push_mu);
        INFQ_ERROR_LOG("[%s]failed to fetch top from push queue in zero mode", infq->name);
        return INFQ_ERR;
    }
    infq_pthread_mutex_unlock(&infq->push_mu);

    return INFQ_OK;
}

int32_t
infq_top(infq_t *infq, void *buf, int32_t buf_size, int32_t *sizeptr)
{
    if (infq == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void  *dataptr;
    if (infq_top_zero_cp(infq, &dataptr, sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to fetch top", infq->name);
        return INFQ_ERR;
    }

    if (*sizeptr > buf_size) {
        INFQ_ERROR_LOG("[%s]buffer is not enough, buf size: %d, expect: %d",
                infq->name,
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    memcpy(buf, dataptr, *sizeptr);

    return INFQ_OK;
}

int32_t
infq_just_pop(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    const void      *dataptr;
    int32_t         size;

    if (infq_pop_zero_cp(infq, &dataptr, &size) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to call pop in zero mode", infq->name);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_push_queue_jump(infq_t *infq)
{
    if (infq == NULL) {
       INFQ_ERROR_LOG("invalid param");
       return INFQ_ERR;
    }

    if (mem_queue_jump(&infq->push_queue) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to make push queue jump to next block");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_size(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return -1;
    }

    // TODO: need to synchronize
    return infq->push_queue.ele_count + infq->pop_queue.ele_count + infq->file_queue.ele_count;
}

int32_t
infq_msize(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_UNDEF;
    }

    // TODO: need to include offset array and index block
    return (infq->push_queue.block_num + infq->pop_queue.block_num) * infq->mem_block_size + \
            2 * sizeof(infq->push_queue) + infq->file_queue.block_num * sizeof(file_block_t);
}

int32_t
infq_fsize(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_UNDEF;
    }

    return infq->file_queue.total_fsize;
}

void
infq_destroy(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    if (bg_exec_destroy(&infq->dump_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy dumper", infq->name);
    }
    if (bg_exec_destroy(&infq->load_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy loader", infq->name);
    }
    if (bg_exec_destroy(&infq->unlink_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy unlinker", infq->name);
    }

    mem_queue_destroy(&infq->push_queue);
    mem_queue_destroy(&infq->pop_queue);

    file_queue_destroy(&infq->file_queue);
    pthread_mutex_destroy(&infq->push_mu);
    pthread_mutex_destroy(&infq->pop_mu);
    if (infq->tmp_mem_block != NULL) {
        mem_block_destroy(infq->tmp_mem_block);
    }

    if (infq->dump_meta_double_buf != NULL) {
        munmap(infq->dump_meta_double_buf, sizeof(infq_dump_meta_t) * 2);
        infq->dump_meta_double_buf = NULL;
    }

    free(infq);
    INFQ_INFO_LOG("[%s]successful to destory infq");
}

int32_t
infq_destroy_completely(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char    name[INFQ_NAME_MAX_LEN];

    if (bg_exec_destroy(&infq->dump_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy dumper", infq->name);
    }
    if (bg_exec_destroy(&infq->load_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy loader", infq->name);
    }
    if (bg_exec_destroy(&infq->unlink_exec) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to destroy unlinker", infq->name);
    }

    mem_queue_destroy(&infq->push_queue);
    mem_queue_destroy(&infq->pop_queue);

    if (file_queue_destroy_completely(&infq->file_queue) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to remove files", infq->name);
        return INFQ_ERR;
    }
    pthread_mutex_destroy(&infq->push_mu);
    pthread_mutex_destroy(&infq->pop_mu);
    if (infq->tmp_mem_block != NULL) {
        mem_block_destroy(infq->tmp_mem_block);
    }

    if (infq->dump_meta_double_buf != NULL) {
        munmap(infq->dump_meta_double_buf, sizeof(infq_dump_meta_t) * 2);
        infq->dump_meta_double_buf = NULL;
    }

    strcpy(name, infq->name);

    free(infq);

    INFQ_INFO_LOG("[%s]successful to destory infq completely", name);

    return INFQ_OK;
}

int32_t
infq_dump(infq_t *infq, char *buf, int32_t buf_size, int32_t *data_size) {
    if (infq == NULL || buf == NULL || data_size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    long long           t1, t2, t3;
    unsigned long       file_path_len, infq_name_len;

    // 0. check whether the buffer is big enough
    file_path_len = strlen(infq->file_queue.file_path) + 1;
    infq_name_len = strlen(infq->name) + 1;
    if ((unsigned long)buf_size < sizeof(infq_dump_meta_t) + INFQ_DUMP_META_LEN
            + file_path_len + infq_name_len) {
        INFQ_ERROR_LOG("[%s]no enough memory, buf size: %d, expected: %d",
                infq->name,
                buf_size,
                sizeof(infq_dump_meta_t) + INFQ_DUMP_META_LEN + file_path_len + infq_name_len);
        return INFQ_ERR;
    }

    // 1. dump push queue
    t1 = time_us();
    if (dump_push_queue(infq) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to dump push queue", infq->name);
        return INFQ_ERR;
    }

    // 2. dump pop queue if it's necessary
    t2 = time_us();
    infq_dump_meta_t       *meta;
    meta = &backup_dump_meta(infq);
    if (dump_pop_queue_if_need(infq, &meta->popq_meta) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to dump pop queue", infq->name);
        return INFQ_ERR;
    }
    t3 = time_us();

    // 3. meta data
    meta->file_path_len = file_path_len;
    meta->infq_name_len = infq_name_len;
    meta->file_path = infq->file_queue.file_path;
    meta->infq_name = infq->name;
    meta->global_ele_idx = infq->global_ele_idx;
    meta->file_meta.block_num = infq->file_queue.block_num;
    meta->file_meta.ele_count = infq->file_queue.ele_count;
    if (!file_queue_empty(&infq->file_queue)) {
        meta->file_meta.file_range.start = infq->file_queue.block_head->suffix;
        // end means exclude boundary
        meta->file_meta.file_range.end = infq->file_queue.block_tail->suffix + 1;
    } else {
        meta->file_meta.file_range.start = INFQ_UNDEF;
        meta->file_meta.file_range.end = INFQ_UNDEF;
    }
    meta->file_meta.file_size = infq->file_queue.total_fsize;

    meta->popq_meta.ele_count = infq->pop_queue.ele_count;
    meta->popq_meta.min_idx = infq->pop_queue.min_idx;
    meta->popq_meta.max_idx = infq->pop_queue.max_idx;
    meta->popq_meta.block_num = infq->pop_queue.block_num;
    meta->popq_meta.block_size = first_block(&infq->pop_queue)->mem_size;

    INFQ_INFO_LOG("[%s]successful to dump infq, total: %lldus, pushq: %lldus, popq: %lldus",
            infq->name,
            t3 - t1,
            t2 - t1,
            t3 - t2);

    strcpy(buf, INFQ_MAGIC_NUMBER);
    strcpy(buf + 8, INFQ_VERSION);
    memmove(buf + INFQ_DUMP_META_LEN, meta, sizeof(infq_dump_meta_t));
    memmove(buf + INFQ_DUMP_META_LEN + sizeof(infq_dump_meta_t),
            infq->file_queue.file_path, file_path_len);
    *data_size = sizeof(infq_dump_meta_t) + INFQ_DUMP_META_LEN + file_path_len;
    memmove(buf + *data_size, infq->name, infq_name_len);
    *data_size += infq_name_len;

    return INFQ_OK;
}

int32_t
infq_load(infq_t *infq, const char *buf, int32_t buf_size) {
    if (infq == NULL || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    infq_dump_meta_t    *meta;
    long long           t1, t2, t3;
    int32_t             blk_size;

    // 0. check the buffer size, magic number, version number
    if (buf_size < (int)sizeof(infq_dump_meta_t) + INFQ_DUMP_META_LEN) {
        INFQ_ERROR_LOG("[%s]buffer is too samll", infq->name);
        return INFQ_ERR;
    }

    if (strncmp(buf, INFQ_MAGIC_NUMBER, strlen(INFQ_MAGIC_NUMBER)) != 0) {
        char    tmp[strlen(INFQ_MAGIC_NUMBER) + 1];
        strncpy(tmp, buf, strlen(INFQ_MAGIC_NUMBER) + 1);
        INFQ_ERROR_LOG("[%s]not a buffer dumped by infq, magic num: %s", infq->name, tmp);
        return INFQ_ERR;
    }

    // TODO: version number check
    if (strncmp(buf + 8, INFQ_VERSION, strlen(INFQ_VERSION)) != 0) {
        INFQ_ERROR_LOG("[%s]not a supported version", infq->name);
    }

    meta = (infq_dump_meta_t *)(buf + INFQ_DUMP_META_LEN);
    if ((unsigned long)buf_size < dump_meta_len(meta)) {
        INFQ_ERROR_LOG("[%s]buffer is too small, buf size: %d, expected: %d",
                infq->name,
                buf_size,
                dump_meta_len(meta));
        return INFQ_ERR;
    }
    meta->file_path = buf + INFQ_DUMP_META_LEN + sizeof(infq_dump_meta_t);
    meta->infq_name = meta->file_path + meta->file_path_len;
    strcpy(infq->name, meta->infq_name);

    // 1. reinit push queue, to clear the old data
    blk_size = first_block(&infq->push_queue)->mem_size;
    mem_queue_destroy(&infq->push_queue);
    if (mem_queue_init(
                &infq->push_queue,
                infq->push_queue.block_num,
                blk_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init push queue", infq->name);
        return INFQ_ERR;
    }
    mem_queue_add_push_blk_cb(&infq->push_queue, full_block_push_callback, infq);

    t1 = time_us();
    // 2. load file queue
    if (load_file_queue(infq, meta) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to load file queue", infq->name);
        return INFQ_ERR;
    }

    t2 = time_us();
    // 3. load pop queue
    if (load_pop_queue(infq, meta) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to load pop queue", infq->name);
        return INFQ_ERR;
    }
    t3 = time_us();

    INFQ_INFO_LOG("[%s]successful to load infq, total: %dus, fileq: %dus, popq: %dus",
            infq->name,
            t3 - t1,
            t2 - t1,
            t3 - t2);

    infq->global_ele_idx = meta->global_ele_idx;
    // push queue is empty, idx range: [n, n)
    infq->push_queue.min_idx = infq->push_queue.max_idx = meta->global_ele_idx;
    // try to load file block to memory as soon as possible
    check_and_trigger_loader(infq);

    // cp dump meta
    memmove(&cur_dump_meta(infq), meta, sizeof(infq_dump_meta_t));

    // update pop_block_suffix
    infq->pop_block_suffix = meta->popq_meta.file_range.end;

    return INFQ_OK;
}

int32_t
infq_done_dump(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_suffix_range   suffix_range[2];
    char*               prefixes[2];
    unlink_job_t        *job_info;
    int                 counter;

    // 1. add diff files to unlinker job queue
    to_rm_files_range(
            &cur_dump_meta(infq).file_meta.file_range,
            &backup_dump_meta(infq).file_meta.file_range,
            &suffix_range[0]);
    INFQ_DEBUG_LOG("need to rm file blocks: [%d, %d)",
            suffix_range[0].start,
            suffix_range[0].end);
    prefixes[0] = INFQ_FILE_BLOCK_PREFIX;
    to_rm_files_range(
            &cur_dump_meta(infq).popq_meta.file_range,
            &backup_dump_meta(infq).popq_meta.file_range,
            &suffix_range[1]);
    INFQ_DEBUG_LOG("need to rm pop blocks: [%d, %d)",
            suffix_range[1].start,
            suffix_range[1].end);
    prefixes[1] = INFQ_POP_BLOCK_PREFIX;

    counter = 0;
    for (int i = 0; i < 2; i++) {
        for (int j = suffix_range[i].start; j < suffix_range[i].end; j++) {
            job_info = (unlink_job_t *)malloc(sizeof(unlink_job_t));
            if (job_info == NULL) {
                INFQ_ERROR_LOG("[%s]failed to alloc mem for unlink job info", infq->name);
                return INFQ_ERR;
            }

            job_info->file_prefix = prefixes[i];
            job_info->file_block_no = j;
            job_info->file_path = infq->file_queue.file_path;
            if (bg_exec_add_job(
                    &infq->unlink_exec,
                    unlink_job,
                    job_info,
                    job_info_destroy,
                    unlink_job_tostr) == INFQ_ERR) {
                INFQ_ERROR_LOG("[%s]failed to add unlink job to bg executor", infq->name);
                return INFQ_ERR;
            }

            counter++;
        }
    }

    // 2. switch dump meta
    infq->cur_meta_idx = 1 - infq->cur_meta_idx;

    // 3. update pop_block_suffix
    infq->pop_block_suffix += cur_dump_meta(infq).popq_meta.file_range.end
            - cur_dump_meta(infq).popq_meta.file_range.start;

    INFQ_INFO_LOG("[%s]add %d files to unlinker executor, meta idx: %d, "
            "cur meta files: [%d, %d), cur meta pop blks: [%d, %d), "
            "backup meta files: [%d, %d), backup meta pop blks: [%d, %d)"
            "file blocks: [%d, %d), pop blocks: [%d, %d)",
            infq->name,
            counter,
            infq->cur_meta_idx,
            cur_dump_meta(infq).file_meta.file_range.start,
            cur_dump_meta(infq).file_meta.file_range.end,
            cur_dump_meta(infq).popq_meta.file_range.start,
            cur_dump_meta(infq).popq_meta.file_range.end,
            backup_dump_meta(infq).file_meta.file_range.start,
            backup_dump_meta(infq).file_meta.file_range.end,
            backup_dump_meta(infq).popq_meta.file_range.start,
            backup_dump_meta(infq).popq_meta.file_range.end,
            suffix_range[0].start,
            suffix_range[0].end,
            suffix_range[1].start,
            suffix_range[1].end);

    return INFQ_OK;
}

/**
 * This function is not thread-safe.
 * However, the caller should guarantee that it has get all the locks.
 */
void
swap_mem_block(infq_t *infq)
{
    int32_t         free_block_num, i, counter;
    mem_block_t     *block;
    mem_queue_t     *pushq, *popq;

    pushq = &infq->push_queue;
    popq = &infq->pop_queue;
    counter = 0;

    free_block_num = mem_queue_free_block_num(popq);

    // NOTICE: make sure last block in pop queue is empty
    if (!mem_block_empty(last_block(popq))) {
        // popq only has the last one block, and the block is not empty.
        // so it can't swap with push queue
        if (popq->first_block == (popq->last_block + 2) % popq->block_num) {
            return;
        }

        free_block_num--;
        if (!mem_queue_full(popq)) {
            popq->last_block = (popq->last_block + 1) % popq->block_num;
        }
    }

    if (free_block_num == 0) {
        return;
    }

    for (i = 0; i < free_block_num; i++) {
        // no full block
        if (!mem_queue_has_full_block(pushq)) {
            break;
        }

        block = first_block(pushq);
        if (mem_block_empty(block)) {
            pushq->first_block = (pushq->first_block + 1) % pushq->block_num;
            continue;
        }

        first_block(pushq) = last_block(popq);
        last_block(popq) = block;

        pushq->first_block = (pushq->first_block + 1) % pushq->block_num;
        popq->last_block = (popq->last_block + 1) % popq->block_num;

        mem_block_reset(last_block(popq), INFQ_UNDEF);

        // update max_idx and min_idx
        if (popq->min_idx == INFQ_UNDEF) {
            popq->min_idx = pushq->min_idx;
        }
        if (popq->max_idx != INFQ_UNDEF) {
#ifdef D_ASSERT
            char    buf[2048];
            INFQ_ASSERT(popq->max_idx == pushq->min_idx,
                    "[%s]max idx not match, block ele count: %d, %s",
                    infq->name, block->ele_count, infq_debug_info(infq, buf, 2048));
#else
            if (popq->max_idx != pushq->min_idx) {
                INFQ_ERROR_LOG("[%s]max idx not match, block ele count: %d, popq max id: %lld, "
                        "pushq min idx: %lld",
                        infq->name,
                        block->ele_count,
                        popq->max_idx,
                        pushq->min_idx);
            }
#endif
        }
        pushq->min_idx += block->ele_count;
        popq->max_idx = pushq->min_idx;
        pushq->ele_count -= block->ele_count;
        popq->ele_count += block->ele_count;

        INFQ_DEBUG_LOG("[%s]popq, min: %lld, max:%lld, block start: %lld, f: %d, l: %d, count: %d",
                infq->name,
                popq->min_idx,
                popq->max_idx,
                block->start_index,
                popq->first_block,
                popq->last_block,
                popq->ele_count);
        INFQ_DEBUG_LOG("[%s]pushq, min: %lld, max: %lld, f: %d, l: %d, count: %d, blk count: %d",
                infq->name,
                pushq->min_idx,
                pushq->max_idx,
                pushq->first_block,
                pushq->last_block,
                pushq->ele_count,
                block->ele_count);

        counter++;
    }

    if (counter > 0) {
        INFQ_DEBUG_LOG("[%s]successful to swap %d memory block between push and pop queue",
                infq->name,
                counter);
    }
}

int32_t
dump_job(void *arg)
{
    if (arg == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    struct dump_job_t   *job_info;
    mem_block_t         *block;
    mem_queue_t         *queue;

    int64_t             min_sidx = INFQ_UNDEF, max_sidx = INFQ_UNDEF;
    int32_t             idx, counter = 0;

    job_info = (struct dump_job_t *)arg;
    if (job_info->infq == NULL) {
        INFQ_ERROR_LOG("[%s]infq of dump job info is null", job_info->infq->name);
        return INFQ_ERR;
    }

    queue = &job_info->infq->push_queue;

    idx = job_info->start_block;
    while (idx != job_info->end_block) {
        block = queue->blocks[idx];
        if (block == NULL) {
            INFQ_ERROR_LOG("[%s]mem block is NULL, block index: %d",
                    job_info->infq->name,
                    job_info->start_block);
            return INFQ_ERR;
        }

        if (file_queue_dump_block(&job_info->infq->file_queue, block) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to dump memory block in background, index: %lld",
                    job_info->infq->name,
                    block->start_index);
            return INFQ_ERR;
        }

        infq_pthread_mutex_lock(&job_info->infq->push_mu);
        // NOTICE: first_block <= last_block
        queue->first_block = (queue->first_block + 1) % queue->block_num;
        queue->min_idx = first_block(queue)->start_index;
        queue->ele_count -= block->ele_count;
        infq_pthread_mutex_unlock(&job_info->infq->push_mu);

        if (min_sidx == INFQ_UNDEF) {
            min_sidx = block->start_index;
        }

        if (max_sidx < block->start_index + block->ele_count) {
            max_sidx = block->start_index + block->ele_count;
        }

        counter++;
        idx = (idx + 1) % job_info->block_num;
    }

    INFQ_INFO_LOG("[%s]succefully to dump %d blocks, file block suffix: [%d, %d], "
            "index range to dump: [%lld, %lld), file blocks: %d",
            job_info->infq->name,
            counter,
            job_info->infq->file_queue.block_head->suffix,
            job_info->infq->file_queue.block_tail->suffix,
            min_sidx,
            max_sidx,
            job_info->infq->file_queue.block_num);

    return INFQ_OK;
}

int32_t
load_job(void *arg)
{
    if (arg == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    struct load_job_t   *job_info;
    mem_block_t         *block;
    mem_queue_t         *queue;
    file_queue_t        *file_queue;
    file_block_t        *head_blk;
    int32_t             i;

    int64_t             min_sidx = INFQ_UNDEF, max_sidx = INFQ_UNDEF;

    job_info = (struct load_job_t *)arg;
    if (job_info->infq == NULL) {
        INFQ_ERROR_LOG("[%s]infq of load job info is null", job_info->infq->name);
        return INFQ_ERR;
    }

    queue = &job_info->infq->pop_queue;
    file_queue = &job_info->infq->file_queue;

    for (i = job_info->file_start_block; ; i++) {
        infq_pthread_mutex_lock(&job_info->infq->pop_mu);
        if (mem_queue_full(queue)) {
            infq_pthread_mutex_unlock(&job_info->infq->pop_mu);
            break;
        }
        infq_pthread_mutex_unlock(&job_info->infq->pop_mu);

        infq_pthread_mutex_lock(&file_queue->mu);
        head_blk = file_queue->block_head;
        infq_pthread_mutex_unlock(&file_queue->mu);
        if (head_blk == NULL || head_blk->suffix == job_info->file_end_block) {
            break;
        }

        if (head_blk->suffix != i) {
            INFQ_ERROR_LOG("[%s]failed to load job. job and file queue isn't matched, "
                    "job suffix: %d, queue suffix: %d",
                    job_info->infq->name,
                    i,
                    file_queue->block_head->suffix);
            return INFQ_ERR;
        }

        // load file block to temporary memory block, then swap it with last block
        if (file_queue_load_block(file_queue, job_info->infq->tmp_mem_block) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to load file block to memory", job_info->infq->name);
            return INFQ_ERR;
        }

        block = job_info->infq->tmp_mem_block;
        INFQ_DEBUG_LOG("[%s]load job, block info, count: %d, start: %lld",
                job_info->infq->name,
                block->ele_count,
                block->start_index);

        infq_pthread_mutex_lock(&job_info->infq->pop_mu);
        // swap the loaded file block with last block
        block = last_block(queue);
        last_block(queue) = job_info->infq->tmp_mem_block;
        job_info->infq->tmp_mem_block = block;
        block = last_block(queue);

        queue->last_block = (queue->last_block + 1) % queue->block_num;
        mem_block_reset(last_block(queue), INFQ_UNDEF);

        if (queue->min_idx == INFQ_UNDEF) {
            queue->min_idx = block->start_index;
        }

        if (queue->max_idx != INFQ_UNDEF) {
            INFQ_ASSERT(block->start_index == queue->max_idx,
                    "[%s]max idx of pop queue not match, min: %lld, max: %lld, start idx of "
                    "blk: %lld, ele count of blk: %d",
                    job_info->infq->name,
                    queue->min_idx,
                    queue->max_idx,
                    block->start_index,
                    block->ele_count);
        }
        queue->max_idx = block->start_index + block->ele_count;
        queue->ele_count += block->ele_count;

        infq_pthread_mutex_unlock(&job_info->infq->pop_mu);

        if (min_sidx == INFQ_UNDEF) {
            min_sidx = block->start_index;
        }

        if (max_sidx == INFQ_UNDEF ||
                max_sidx < block->start_index + block->ele_count) {
            max_sidx = block->start_index + block->ele_count;
        }

        INFQ_DEBUG_LOG("[%s]load block, start index: %lld, blk count: %d, f: %d, l: %d, "
                "count: %d, min: %lld, max: %lld, last count: %d, last start: %lld",
                job_info->infq->name,
                block->start_index,
                block->ele_count,
                queue->first_block,
                queue->last_block,
                queue->ele_count,
                queue->min_idx,
                queue->max_idx,
                last_block(queue)->ele_count,
                last_block(queue)->start_index);
    }

    if (i > job_info->file_start_block) {
        INFQ_INFO_LOG("[%s]successful to load %d file blocks, block suffix: [%d, %d), path: %s, "
                "idx range: [%lld, %lld), f: %d, l: %d, file blocks: %d",
                job_info->infq->name,
                i - job_info->file_start_block,
                job_info->file_start_block,
                i,
                job_info->infq->file_queue.file_path,
                min_sidx,
                max_sidx,
                queue->first_block,
                queue->last_block,
                job_info->infq->file_queue.block_num);
    }

    return INFQ_OK;
}

/**
 * @brief When there are blocks in file queue and free blocks in pop queue,
 *      load jobs will be added to the job queue of loader.
 */
int32_t
check_and_trigger_loader(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t             free_block_num, job_dup, start_fblock, fblock_num;
    struct load_job_t   *job_info;

    // fetch free block count
    free_block_num = mem_queue_free_block_num(&infq->pop_queue);
    if (free_block_num <= 0) {
        return INFQ_OK;
    }

    // fetch file blocks to load
    infq_pthread_mutex_lock(&infq->file_queue.mu);
    if (infq->file_queue.block_head == NULL) {
        start_fblock = 0;
    } else {
        start_fblock = infq->file_queue.block_head->suffix;
    }
    fblock_num = infq->file_queue.block_num;
    infq_pthread_mutex_unlock(&infq->file_queue.mu);

    // no file blocks to load
    if (fblock_num == 0) {
        return INFQ_OK;
    }

    job_info = (struct load_job_t *)malloc(sizeof(struct load_job_t));
    if (job_info == NULL) {
        INFQ_ERROR_LOG("[%s]failed to alloc memory for load job info", infq->name);
        return INFQ_ERR;
    }

    job_info->infq = infq;
    job_info->file_start_block = start_fblock;
    if (free_block_num > fblock_num) {
        free_block_num = fblock_num;
    }
    job_info->file_end_block = job_info->file_start_block + free_block_num;

    if (bg_exec_distinct_job(&infq->load_exec, load_job_dup_checker, job_info, &job_dup) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to check dup for load job", infq->name);
        return INFQ_ERR;
    }

    if (job_dup == INFQ_FALSE) {
        if (bg_exec_add_job(
                    &infq->load_exec,
                    load_job,
                    job_info,
                    job_info_destroy,
                    load_job_tostr) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to add job to background loader", infq->name);
            return INFQ_ERR;
        }

        INFQ_DEBUG_LOG("[%s]add load job, blocks: [%d, %d)",
                infq->name,
                job_info->file_start_block,
                job_info->file_end_block);
    }

    return INFQ_OK;
}

int32_t
infq_check_q(mem_queue_t *q, pthread_mutex_t *mu)
{
    int32_t     ret;

    infq_pthread_mutex_lock(mu);
    do {
        ret = INFQ_TRUE;
        if (q->max_idx == INFQ_UNDEF || q->min_idx == INFQ_UNDEF) {
            if (q->ele_count != 0) {
                INFQ_ERROR_LOG("check1=> min: %lld, max: %lld, count: %d", q->min_idx,
                        q->max_idx,
                        q->ele_count);
                ret = INFQ_FALSE;
                break;
            }
            break;
        }

        if (q->ele_count != q->max_idx - q->min_idx) {
            INFQ_ERROR_LOG("check2=> min: %lld, max: %lld, count: %d", q->min_idx,
                    q->max_idx,
                    q->ele_count);
            ret = INFQ_FALSE;
            break;
        }
    } while (0);

    infq_pthread_mutex_unlock(mu);

    return ret;
}

int32_t
infq_check_pushq(infq_t *infq)
{
    return infq_check_q(&infq->push_queue, &infq->push_mu);
}

int32_t
infq_check_popq(infq_t *infq)
{
    return infq_check_q(&infq->pop_queue, &infq->pop_mu);
}

int32_t
dump_push_queue(infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t         idx;
    mem_block_t     *block;

    idx = infq->push_queue.first_block;
    while (1) {
        block = infq->push_queue.blocks[idx];
        if (!mem_block_empty(block)) {
            if (file_queue_dump_block(&infq->file_queue, block) == INFQ_ERR) {
                INFQ_ERROR_LOG("[%s]failed to dump push mem block, idx: %d", infq->name, idx);
                return INFQ_ERR;
            }
            mem_block_reset(block, INFQ_UNDEF);
            INFQ_INFO_LOG("[%s]infq persistent dump, push queue, suffix: %d, start_index: %lld, "
                    "ele_count: %d",
                    infq->name,
                    infq->file_queue.block_tail->suffix,
                    infq->file_queue.block_tail->start_index,
                    infq->file_queue.block_tail->ele_count);
        }

        if (idx == infq->push_queue.last_block) {
            break;
        }
        idx = (idx + 1) % infq->push_queue.block_num;
    }

    // NOTCIE: remember the index for push queue, used by at function
    infq->push_queue.min_idx = infq->push_queue.max_idx;
    infq->push_queue.first_block = infq->push_queue.last_block;
    infq->push_queue.ele_count = 0;

    return INFQ_OK;
}

int32_t
link_pop_block_to_file(infq_t *infq, mem_block_t *block, int32_t blk_counter)
{
    char            file_block_path[INFQ_MAX_BUF_SIZE];
    char            pop_block_path[INFQ_MAX_BUF_SIZE];
    unsigned char   file_sign[20], blk_sign[20];

    if (gen_file_path(
                infq->file_queue.file_path,
                INFQ_FILE_BLOCK_PREFIX,
                block->file_block_no,
                file_block_path,
                INFQ_MAX_BUF_SIZE) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to generate file block file path", infq->name);
        return INFQ_ERR;
    }

    // check signature
    if (file_fetch_signature(file_block_path, file_sign) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to fetch file sign, path: %s",
                infq->name,
                file_block_path);
        return INFQ_ERR;
    }

    if (mem_block_signature(block, blk_sign) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to fetch sign of mem block", infq->name);
        return INFQ_ERR;
    }

    // mem block and file are not match, the block need to dump again
    if (memcmp(file_sign, blk_sign, INFQ_SIGNATURE_LEN) != 0) {
        INFQ_ERROR_LOG("[%s]signature of mem block and file not match", infq->name);
        return INFQ_ERR;
    }

    if (gen_file_path(
                infq->file_queue.file_path,
                INFQ_POP_BLOCK_PREFIX,
                blk_counter,
                pop_block_path,
                INFQ_MAX_BUF_SIZE) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to generate pop block file path", infq->name);
        return INFQ_ERR;
    }
    // check the pop block file path to see if it exists, remove it
    // if it exists
    if (access(pop_block_path, F_OK) != -1) {
        INFQ_DEBUG_LOG("pop block file already exits, file path: %s", pop_block_path);
        if (unlink(pop_block_path) == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("[%s]failed to unlink file, file path: %s",
                    infq->name,
                    pop_block_path);
            return INFQ_ERR;
        }
        INFQ_DEBUG_LOG("pop block file unlinked, file path: %s", pop_block_path);
    }

    if (link(file_block_path, pop_block_path) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("[%s]failed to link file %s to %s",
                infq->name,
                file_block_path,
                pop_block_path);
        return INFQ_ERR;
    }
    INFQ_INFO_LOG("[%s]infq persistent dump, pop queue, link file block: %d, "
            "start index: %lld, ele count: %d, pop suffix: %d",
            infq->name,
            block->file_block_no,
            block->start_index,
            block->ele_count,
            blk_counter);

    return INFQ_OK;
}

int32_t
dump_pop_queue_if_need(infq_t *infq, popq_dump_meta_t *meta)
{
    if (infq == NULL || meta == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t         idx, blk_counter;
    mem_block_t     *block;
    file_block_t    fblock;

    idx = infq->pop_queue.first_block;
    blk_counter = infq->pop_block_suffix;
    // NOTICE: The last block in pop queue is always empty,
    //          so no need to dump it.
    while (idx != infq->pop_queue.last_block) {
        block = infq->pop_queue.blocks[idx];
        if (mem_block_empty(block)) {
            break;
        }

        // 1. try to use the dumped file
        //
        // TODO: only link the file when the pop mem block hasn't been modified, or
        //      the file and mem block are not consistent.
        //
        // check the mem block to see if it's loaded from file queue, link
        // the file block file to pop block file instead of dumping the mem
        // block again
        if (block->file_block_no != INFQ_UNDEF) {
            if (link_pop_block_to_file(infq, block, blk_counter) == INFQ_OK) {
                blk_counter++;
                idx = (idx + 1) % infq->pop_queue.block_num;
                continue;
            }

            // failed to use dumped file, just dump a new file block
        }

        // 2. dump pop block
        // the mem block is swapped from push queue, dump the mem block to
        // pop block file
        if (file_block_init(
                    &fblock,
                    infq->file_queue.file_path,
                    INFQ_POP_BLOCK_PREFIX) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to init file block", infq->name);
            return INFQ_ERR;
        }

        if (file_block_write(&fblock, blk_counter, block) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to write file block", infq->name);
            return INFQ_ERR;
        }

        file_block_destroy(&fblock);
        INFQ_INFO_LOG("[%s]infq persistent dump, pop queue, suffix: %d, "
                "start index: %lld, ele count: %d",
                infq->name,
                blk_counter,
                block->start_index,
                block->ele_count);

        blk_counter++;
        idx = (idx + 1) % infq->pop_queue.block_num;
    }

    meta->file_range.start = infq->pop_block_suffix;
    meta->file_range.end = blk_counter;

    return INFQ_OK;
}

int32_t
load_file_queue(infq_t *infq, infq_dump_meta_t *meta)
{
    if (infq == NULL || meta == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_queue_destroy(&infq->file_queue);
    if (file_queue_init(&infq->file_queue, meta->file_path) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init file queue in infq_load", infq->name);
        return INFQ_ERR;
    }
    for (int i = meta->file_meta.file_range.start;
            i < meta->file_meta.file_range.end; i++) {
        if (file_queue_add_block_by_file(&infq->file_queue, i) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to add block by file", infq->name);
            return INFQ_ERR;
        }
    }

    INFQ_ASSERT(infq->file_queue.block_num == meta->file_meta.block_num,
            "[%s]load err, block count not matched, meta: %d, load: %d,",
            infq->name,
            meta->file_meta.block_num,
            infq->file_queue.block_num);
    INFQ_ASSERT(infq->file_queue.ele_count == meta->file_meta.ele_count,
            "[%s]load err, element count not matched, meta: %d, load: %d",
            infq->name,
            meta->file_meta.ele_count,
            infq->file_queue.ele_count);
    infq->file_queue.block_suffix = meta->file_meta.file_range.end == INFQ_UNDEF ? 0 :
            meta->file_meta.file_range.end;

    return INFQ_OK;
}

int32_t
load_pop_queue(infq_t *infq, infq_dump_meta_t *meta)
{
    if (infq == NULL || meta == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t         min_idx, max_idx;
    file_block_t    fblock;
    mem_block_t     *mblock;

    /**
     * Only reinit pop queue when current block size is too small.
     * However, infQ loading is a heavy operation, the cost of reinit
     * pop queue is insignificant.
     */
    mem_queue_destroy(&infq->pop_queue);
    if (mem_queue_init(
                &infq->pop_queue,
                meta->popq_meta.block_num,
                meta->popq_meta.block_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to init pop queue", infq->name);
        return INFQ_ERR;
    }
    mem_queue_add_pop_blk_cb(&infq->pop_queue, empty_block_pop_callback, infq);

    min_idx = max_idx = INFQ_UNDEF;
    for (int i = meta->popq_meta.file_range.start;
            i < meta->popq_meta.file_range.end; i++) {
        if (file_block_init(&fblock, meta->file_path, INFQ_POP_BLOCK_PREFIX) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to init file block", infq->name);
            return INFQ_ERR;
        }
        fblock.suffix = i;

        if (mem_queue_full(&infq->pop_queue)) {
            INFQ_ERROR_LOG("[%s]pop queue is full, no enough blocks", infq->name);
            return INFQ_ERR;
        }
        mblock = last_block(&infq->pop_queue);

        if (file_block_load(&fblock, mblock) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed load file in load infq, suffix: %d", infq->name, i);
            return INFQ_ERR;
        }

        int32_t e = mblock->start_index + mblock->ele_count;
        if (max_idx == INFQ_UNDEF || max_idx < e) {
            max_idx = e;
        }
        if (min_idx == INFQ_UNDEF || min_idx > mblock->start_index) {
            min_idx = mblock->start_index;
        }

        infq->pop_queue.ele_count += mblock->ele_count;
        infq->pop_queue.last_block = (infq->pop_queue.last_block + 1) % infq->pop_queue.block_num;

        // free the memory of the offset array belongs to file block
        file_block_destroy(&fblock);
    }

    // NOTICE: load成功后就删除pop block的文件，如果失败，会丢数据
    //      延迟删除
    // TODO: delay deletion of pop block files
    // success, remove all the files belongs to pop queue
    /*for (int i = meta->popq_meta.file_range.start;*/
    /*        i < meta->popq_meta.file_range.end; i++) {*/
    /*    if (file_block_init(&fblock, meta->file_path, INFQ_POP_BLOCK_PREFIX) == INFQ_ERR) {*/
    /*        INFQ_ERROR_LOG("[%s]failed to init file block", infq->name);*/
    /*        return INFQ_ERR;*/
    /*    }*/
    /*    fblock.suffix = i;*/

    /*    file_block_file_delete(&fblock);*/
    /*}*/

    if (min_idx != INFQ_UNDEF && max_idx != INFQ_UNDEF) {
        INFQ_ASSERT(max_idx == meta->popq_meta.max_idx && min_idx == meta->popq_meta.min_idx,
                "[%s]load err. meta data of pop queue not match, load[max: %lld, min: %lld, ele: %d],"
                "meta[max: %lld, min: %lld, ele: %d]",
                infq->name,
                max_idx,
                min_idx,
                infq->pop_queue.ele_count,
                meta->popq_meta.max_idx,
                meta->popq_meta.min_idx,
                meta->popq_meta.ele_count);
    }

    INFQ_ASSERT(meta->popq_meta.ele_count == infq->pop_queue.ele_count,
            "[%s]index range match, but ele_count not match, meta[max: %lld, min: %lld, ele: %d],"
            "load[ele: %d]",
            infq->name,
            meta->popq_meta.min_idx,
            meta->popq_meta.max_idx,
            meta->popq_meta.ele_count,
            infq->pop_queue.ele_count);

    infq->pop_queue.min_idx = min_idx;
    infq->pop_queue.max_idx = max_idx;

    return INFQ_OK;
}

infq_dump_meta_t*
infq_fetch_dump_meta(infq_t *infq)
{
    if (infq == NULL ) {
        INFQ_ERROR_LOG("invalid param");
        return NULL;
    }

    return &cur_dump_meta(infq);
}

int32_t
infq_fetch_stats(infq_t *infq, infq_stats_t *stats)
{
    if (infq == NULL || stats == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    stats->mem_size = infq_msize(infq);
    stats->file_size = infq_fsize(infq);
    stats->mem_block_size = first_block(&infq->push_queue)->mem_size;
    stats->pushq_blocks_num = infq->push_queue.block_num;
    stats->popq_blocks_num = infq->pop_queue.block_num;
    stats->pushq_used_blocks = mem_queue_full_block_num(&infq->push_queue);
    if (!mem_block_empty(last_block(&infq->push_queue))) {
        stats->pushq_used_blocks++;
    }
    stats->popq_used_blocks = mem_queue_full_block_num(&infq->pop_queue);
    if (!mem_block_empty(last_block(&infq->pop_queue))) {
        stats->popq_used_blocks++;
    }
    stats->fileq_blocks_num = infq->file_queue.block_num;
    stats->dumper.job_num = bg_exec_pending_task_num(&infq->dump_exec);
    stats->loader.job_num = bg_exec_pending_task_num(&infq->load_exec);
    stats->unlinker.job_num = bg_exec_pending_task_num(&infq->unlink_exec);
    stats->dumper.is_suspended = infq->dump_exec.suspended;
    stats->loader.is_suspended = infq->load_exec.suspended;
    stats->unlinker.is_suspended = infq->unlink_exec.suspended;

    return INFQ_OK;
}

const char*
infq_debug_info(infq_t *infq, char *buf, int32_t size)
{
    if (infq == NULL || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return NULL;
    }

    int32_t         ret, last_blk_idx;
    char            first_buf[1024], last_buf[1024];
    char            push_first[1024], push_last[1024];
    char            pop_first[1024], pop_last[1024];

    last_blk_idx = (infq->pop_queue.last_block - 1) % infq->pop_queue.block_num;
    if (last_blk_idx < infq->pop_queue.first_block) {
        pop_last[0] = '\0';
    } else {
        if (mem_block_debug_info(infq->pop_queue.blocks[last_blk_idx], pop_last, 1024) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to fetch last block of popq");
            pop_last[0] = '\0';
        }
    }

    if (mem_block_debug_info(last_block(&infq->push_queue), push_last, 1024) == INFQ_ERR ||
            mem_block_debug_info(first_block(&infq->push_queue), push_first, 1024) == INFQ_ERR ||
            mem_block_debug_info(first_block(&infq->pop_queue), pop_first, 1024) == INFQ_ERR ||
            file_block_debug_info(infq->file_queue.block_head, first_buf, 1024) == INFQ_ERR ||
            file_block_debug_info(infq->file_queue.block_tail, last_buf, 1024) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to fetch block debug info");
        return NULL;
    }

    ret = snprintf(buf, size, "\r\n{\r\n"
            "  global_ele_idx: %lld,\r\n"
            "  push: {\r\n"
            "    first_block_idx: %d,\r\n"
            "    last_block_idx: %d,\r\n"
            "    ele_count: %d,\r\n"
            "    min_idx: %lld,\r\n"
            "    max_idx: %lld,\r\n"
            "    first block: {\r\n"
            "%s"
            "    }, last block: {\r\n"
            "%s"
            "    }\r\n"
            "  }, pop: {\r\n"
            "    first_block_idx: %d,\r\n"
            "    last_block_idx: %d,\r\n"
            "    ele_count: %d,\r\n"
            "    min_idx: %lld,\r\n"
            "    max_idx: %lld,\r\n"
            "    first block: {\r\n"
            "%s"
            "    }, last block: {\r\n"
            "%s"
            "    }\r\n"
            "  }, file: {\r\n"
            "    blocks_num: %d,\r\n"
            "    ele_count: %d,\r\n"
            "    block_suffix: %d,\r\n"
            "    first block: {\r\n"
            "%s"
            "    }, last block: {\r\n"
            "%s"
            "    }\r\n"
            "}\r\n",
            infq->global_ele_idx,
            infq->push_queue.first_block,
            infq->push_queue.last_block,
            infq->push_queue.ele_count,
            infq->push_queue.min_idx,
            infq->push_queue.max_idx,
            push_first,
            push_last,
            infq->pop_queue.first_block,
            infq->pop_queue.last_block,
            infq->pop_queue.ele_count,
            infq->pop_queue.min_idx,
            infq->pop_queue.max_idx,
            pop_first,
            pop_last,
            infq->file_queue.block_num,
            infq->file_queue.ele_count,
            infq->file_queue.block_suffix,
            first_buf,
            last_buf);

    if (ret == -1 || ret >= size) {
        INFQ_ERROR_LOG("buf is too small, buf size: %d, expected: %d",
                size, ret);
        return NULL;
    }

    return buf;
}

int32_t infq_suspend_bg_exec(infq_t *infq, int32_t exec_type)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t ret;

    switch (exec_type) {
        case INFQ_UNLINK_BG_EXEC:
            ret = bg_exec_suspend(&infq->unlink_exec);
            break;
        case INFQ_DUMP_BG_EXEC:
            ret = bg_exec_suspend(&infq->dump_exec);
            break;
        case INFQ_LOAD_BG_EXEC:
            ret = bg_exec_suspend(&infq->load_exec);
            break;
        default:
            INFQ_ERROR_LOG("invalid param");
            return INFQ_ERR;
    }

    if (ret == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to suspend background exec thread, type: %d",
                infq->name,
                exec_type);
        return INFQ_ERR;
    }
    INFQ_INFO_LOG("[%s]successfully suspended the background exec thread for InfQ", infq->name);

    return INFQ_OK;
}

int32_t infq_continue_bg_exec(infq_t *infq, int32_t exec_type)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t ret;

    switch (exec_type) {
        case INFQ_UNLINK_BG_EXEC:
            ret = bg_exec_continue(&infq->unlink_exec);
            break;
        case INFQ_DUMP_BG_EXEC:
            ret = bg_exec_continue(&infq->dump_exec);
            break;
        case INFQ_LOAD_BG_EXEC:
            ret = bg_exec_continue(&infq->load_exec);
            break;
        default:
            INFQ_ERROR_LOG("invalid param");
            return INFQ_ERR;
    }

    if (ret == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to continue background exec thread, type: %d",
                infq->name,
                exec_type);
        return INFQ_ERR;
    }
    INFQ_INFO_LOG("[%s]successfully continued the background exec thread", infq->name);

    return INFQ_OK;
}

int32_t infq_continue_bg_exec_if_suspended(infq_t *infq, int32_t exec_type)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t ret;

    switch (exec_type) {
        case INFQ_UNLINK_BG_EXEC:
            ret = bg_exec_continue_if_suspended(&infq->unlink_exec);
            break;
        case INFQ_DUMP_BG_EXEC:
            ret = bg_exec_continue_if_suspended(&infq->dump_exec);
            break;
        case INFQ_LOAD_BG_EXEC:
            ret = bg_exec_continue_if_suspended(&infq->load_exec);
            break;
        default:
            INFQ_ERROR_LOG("invalid param");
            return INFQ_ERR;
    }

    if (ret == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to continue background exec thread", infq->name);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
empty_block_pop_callback(void *arg, mem_block_t *blk)
{
    if (arg == NULL || blk == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    infq_t  *infq = (infq_t *)arg;

    // NOTICE: 此处不再删除pop掉的block对应的文件
    //      在持久化后，会删除两次持久化diff的文件。主要是使得通过一个
    //      持久化的元数据还可以恢复InfQ。如果此处删除了对应文件，则之后
    //      某一时刻down掉了，通过之前持久化的信息是无法恢复的，因为对应
    //      的文件已经被删除掉
    //
    // add to unlinker if the memory block's data still stored as file
    /*if (blk->file_block_no != INFQ_UNDEF) {*/
    /*    unlink_job_t *job_info;*/

    /*    job_info = (unlink_job_t *)malloc(sizeof(unlink_job_t));*/
    /*    if (job_info == NULL) {*/
    /*        INFQ_ERROR_LOG("[%s]failed to alloc mem for unlink job info", infq->name);*/
    /*        return INFQ_ERR;*/
    /*    }*/
    /*    job_info->file_block_no = mem_block_file_blk_no(blk);*/
    /*    job_info->file_path = infq->file_queue.file_path;*/
    /*    job_info->file_prefix = INFQ_FILE_BLOCK_PREFIX;*/
    /*    if (bg_exec_add_job(*/
    /*                &infq->unlink_exec,*/
    /*                unlink_job,*/
    /*                job_info,*/
    /*                job_info_destroy,*/
    /*                unlink_job_tostr) == INFQ_ERR) {*/
    /*        INFQ_ERROR_LOG("[%s]failed to add unlink job to bg executor", infq->name);*/
    /*        return INFQ_ERR;*/
    /*    }*/
    /*}*/

    // check to see if add a loader job
    if (check_and_trigger_loader(infq) == INFQ_ERR) {
        INFQ_ERROR_LOG("[%s]failed to check and trigger load task", infq->name);
    }

    return INFQ_OK;
}

int32_t
full_block_push_callback(void *arg)
{
    if (arg == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    struct dump_job_t   *job_info;
    infq_t              *infq = (infq_t *)arg;
    int32_t             full_block_num, block_num, job_dup;

    // try to swap mem block with pop queue
    if (file_queue_empty(&infq->file_queue) && !mem_queue_full(&infq->pop_queue)
                && bg_exec_pending_task_num(&infq->dump_exec) == 0
                && bg_exec_pending_task_num(&infq->load_exec) == 0) {
        infq_pthread_mutex_lock(&infq->pop_mu);
        swap_mem_block(infq);
        infq_pthread_mutex_unlock(&infq->pop_mu);
        return INFQ_OK;
    }

    // add full block to background dumper
    full_block_num = mem_queue_full_block_num(&infq->push_queue);

    // NOTICE: 两种情况触发dumper去dump内存块
    //      1) 当文件队列非空时，保证持久化时尽量少做IO操作，最多只会dump一个内存块
    //      2) 文件队列为空时，push queue的使用率达到阈值。在小于阈值前，会swap到
    //          pop queue，尽量走内存
    if (!file_queue_empty(&infq->file_queue) ||
            full_block_num >= infq->push_queue.block_num * infq->block_usage_to_dump) {
        job_info = (struct dump_job_t *)malloc(sizeof(struct dump_job_t));
        if (job_info == NULL) {
            INFQ_ERROR_LOG("[%s]failed to alloc mem for dump job info", infq->name);
            return INFQ_ERR;
        }
        job_info->infq = infq;
        job_info->block_num = infq->push_queue.block_num;
        job_info->start_block = infq->push_queue.first_block;
        job_info->end_block = infq->push_queue.last_block;
        block_num = (job_info->end_block - job_info->start_block + infq->push_queue.block_num) %
                infq->push_queue.block_num;
        if (bg_exec_distinct_job(
                    &infq->dump_exec,
                    dump_job_dup_checker,
                    job_info,
                    &job_dup) == INFQ_ERR) {
            INFQ_ERROR_LOG("[%s]failed to check job existance", infq->name);
            return INFQ_ERR;
        }
        if (job_dup == INFQ_FALSE) {
            if (bg_exec_add_job(
                        &infq->dump_exec,
                        dump_job,
                        job_info,
                        job_info_destroy,
                        dump_job_tostr) == INFQ_ERR) {
                INFQ_ERROR_LOG("[%s]failed to add dump job to bg executor", infq->name);
                return INFQ_ERR;
            }
            INFQ_DEBUG_LOG("[%s]add dump job in background, block num: %d, "
                    "block index: [%lld, %lld), element index: [%lld, %lld)",
                    infq->name,
                    block_num,
                    job_info->start_block,
                    job_info->end_block,
                    first_block(&infq->push_queue)->start_index,
                    last_block(&infq->push_queue)->start_index);
        } else {
            INFQ_DEBUG_LOG("[%s]dup job, [%d, %d], block num: %d",
                    infq->name,
                    job_info->start_block,
                    job_info->end_block,
                    block_num);
        }
    }

    return INFQ_OK;
}
