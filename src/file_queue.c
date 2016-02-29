/**
 *
 * @file    file_queue
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/25 16:01:57
 */

#include <stdlib.h>
#include <string.h>

#include "file_queue.h"
#include "utils.h"

int32_t
file_queue_init(file_queue_t *file_queue, const char *data_path)
{
    if (file_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutexattr_t     mu_attr;
    int32_t                 err;

    memset(file_queue, 0, sizeof(file_queue_t));
    if (file_block_index_init(&file_queue->index) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to init block index");
        goto failed;
    }

    file_queue->file_path = (char *)malloc(strlen(data_path) + 1);
    if (file_queue->file_path == NULL) {
        INFQ_ERROR_LOG("failed to alloc memory for file path");
        goto failed;
    }
    strcpy(file_queue->file_path, data_path);

    if (pthread_mutexattr_init(&mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to init mutex attr");
        goto failed;
    }

    if (pthread_mutexattr_settype(&mu_attr, PTHREAD_MUTEX_ERRORCHECK) != 0) {
        INFQ_ERROR_LOG("failed to set type of mutex attr");
        goto failed;
    }

    if (pthread_mutex_init(&file_queue->mu, &mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to init mu for file queue");
        goto failed;
    }

    if (pthread_mutexattr_destroy(&mu_attr) != 0) {
        INFQ_ERROR_LOG("failed to destory mutex attr");
        goto failed;
    }

    return INFQ_OK;

failed:
    if (file_queue->file_path != NULL) {
        free(file_queue->file_path);
        file_queue->file_path = NULL;
    }

    file_block_index_destroy(&file_queue->index);

    if ((err = pthread_mutex_destroy(&file_queue->mu)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destory mu");
    }

    if ((err = pthread_mutexattr_destroy(&mu_attr)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to desotry mutex attr");
    }

    return INFQ_ERR;
}

// push
int32_t file_queue_dump_block(file_queue_t *file_queue, mem_block_t *mem_block)
{
    if (file_queue == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_block_t *block = (file_block_t *)malloc(sizeof(file_block_t));
    if (block == NULL) {
        INFQ_ERROR_LOG("failed to alloc mem for file block");
        return INFQ_ERR;
    }
    if (file_block_init(block, file_queue->file_path, NULL) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to init file block");
        return INFQ_ERR;
    }

    // dump to file block
    if (file_block_write(block, file_queue->block_suffix, mem_block) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to dump mem block to file block, path: %s, suffix: %d",
                file_queue->file_path,
                file_queue->block_suffix);
        return INFQ_ERR;
    }

    pthread_mutex_lock(&file_queue->mu);
    // update file block chain
    if (file_queue->block_head == NULL || file_queue->block_tail == NULL) {
        file_queue->block_head = file_queue->block_tail = block;
    } else {
        file_queue->block_tail->next = block;
        file_queue->block_tail = block;
    }

    // update file block index
    if (file_block_index_push(&file_queue->index, block) == INFQ_ERR) {
        pthread_mutex_unlock(&file_queue->mu);
        INFQ_ERROR_LOG("failed to push block to index");
        return INFQ_ERR;
    }

    file_queue->block_suffix++;
    file_queue->block_num++;
    file_queue->total_fsize += block->file_size;
    file_queue->ele_count += mem_block->ele_count;
    pthread_mutex_unlock(&file_queue->mu);

    return INFQ_OK;
}

// pop
int32_t file_queue_load_block(file_queue_t *file_queue, mem_block_t *mem_block)
{
    if (file_queue == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (file_queue->block_head == NULL || file_queue->block_num < 1) {
        INFQ_ERROR_LOG("file queue is empty");
        return INFQ_ERR;
    }

    // NOTICE: 不需要加锁。对于head_block只有loader和dumper线程会访问，
    //      最坏情况是，此前head_block是NULL，dumper添加head_block，
    //      而loader只读到NULL
    file_block_t *block = file_queue->block_head;

    if (file_block_load(block, mem_block) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to load file block to mem block, path: %s, suffix: %d",
                block->file_path,
                block->suffix);
        return INFQ_ERR;
    }

    pthread_mutex_lock(&file_queue->mu);
    if (file_block_index_pop(&file_queue->index) == INFQ_ERR) {
        pthread_mutex_unlock(&file_queue->mu);
        INFQ_ERROR_LOG("failed to pop block to index");
        return INFQ_ERR;
    }

    file_queue->block_num--;
    file_queue->block_head = file_queue->block_head->next;
    if (file_queue->block_head == NULL) {
        file_queue->block_tail = NULL;
    }
    file_queue->total_fsize -= block->file_size;
    file_queue->ele_count -= block->ele_count;
    mem_block->file_block_no = block->suffix;
    pthread_mutex_unlock(&file_queue->mu);

    // free the file block
    file_block_destroy(block);
    free(block);

    return INFQ_OK;
}

int32_t file_queue_at(
        file_queue_t *file_queue,
        int64_t global_idx,
        void *buf,
        int32_t buf_size,
        int32_t *size)
{
    if (file_queue == NULL || buf == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_block_t    *file_block;

    infq_pthread_mutex_lock(&file_queue->mu);
    if (file_block_index_search(&file_queue->index, global_idx, &file_block) == INFQ_ERR) {
        infq_pthread_mutex_unlock(&file_queue->mu);
        INFQ_ERROR_LOG("failed to search file block at %d", global_idx);
        return INFQ_ERR;
    }
    infq_pthread_mutex_unlock(&file_queue->mu);

    if (file_block == NULL) {
        INFQ_ERROR_LOG("failed to search file block by index, index: %d", global_idx);
        return INFQ_ERR;
    }

    if (file_block_at(file_block, global_idx, buf, buf_size, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to call at of file block, index: %d, path: %s, suffix: %d",
                global_idx,
                file_block->file_path,
                file_block->suffix);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

void
file_queue_reset(file_queue_t *file_queue) {
    file_queue->ele_count = 0;
    file_queue->total_fsize = 0;
    file_queue->block_num = 0;
    file_queue->block_head = file_queue->block_tail = NULL;
}

void
file_queue_destroy(file_queue_t *file_queue)
{
    if (file_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return;
    }

    file_block_t    *tmp;

    infq_pthread_mutex_lock(&file_queue->mu);
    while (file_queue->block_head != NULL) {
        tmp = file_queue->block_head->next;
        file_block_destroy(file_queue->block_head);
        free(file_queue->block_head);
        file_queue->block_head = tmp;
    }
    file_queue_reset(file_queue);
    infq_pthread_mutex_unlock(&file_queue->mu);

    if (file_queue->file_path != NULL) {
        free(file_queue->file_path);
        file_queue->file_path = NULL;
    }

    pthread_mutex_destroy(&file_queue->mu);
    file_block_index_destroy(&file_queue->index);
}

int32_t
file_queue_destroy_completely(file_queue_t *file_queue)
{
    if (file_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_block_t    *tmp;

    infq_pthread_mutex_lock(&file_queue->mu);
    while (file_queue->block_head != NULL) {
        if (file_block_file_delete(file_queue->block_head) ==INFQ_ERR) {
            INFQ_ERROR_LOG("failed to delete file");
            infq_pthread_mutex_unlock(&file_queue->mu);
            return INFQ_ERR;
        }
        tmp = file_queue->block_head->next;
        file_block_destroy(file_queue->block_head);
        free(file_queue->block_head);
        file_queue->block_head = tmp;
    }
    file_queue_reset(file_queue);
    infq_pthread_mutex_unlock(&file_queue->mu);

    free(file_queue->file_path);
    pthread_mutex_destroy(&file_queue->mu);
    file_block_index_destroy(&file_queue->index);

    return INFQ_OK;
}

int32_t
file_queue_empty(file_queue_t *file_queue)
{
    int32_t     empty;

    pthread_mutex_lock(&file_queue->mu);
    empty = file_queue->block_num == 0;
    pthread_mutex_unlock(&file_queue->mu);

    return empty;
}

int32_t
file_queue_add_block_by_file(file_queue_t *file_queue, int32_t file_suffix)
{
    if (file_queue == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    file_block_t    *block;

    block = (file_block_t *)malloc(sizeof(file_block_t));
    if (block == NULL) {
        INFQ_ERROR_LOG("failed to alloc mem for file block");
        return INFQ_ERR;
    }
    if (file_block_init(block, file_queue->file_path, NULL) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to init file block");
        return INFQ_ERR;
    }

    // add to the chain of file blocks
    infq_pthread_mutex_lock(&file_queue->mu);
    if (file_queue->block_head == NULL || file_queue->block_tail == NULL) {
        file_queue->block_head = file_queue->block_tail = block;
    } else {
        file_queue->block_tail->next = block;
        file_queue->block_tail = block;
    }
    block->suffix = file_suffix;
    file_queue->block_num++;

    // load header
    if (file_block_load_header(block) == INFQ_ERR) {
        infq_pthread_mutex_unlock(&file_queue->mu);
        INFQ_ERROR_LOG("failed to load block header");
        return INFQ_ERR;
    }
    file_queue->ele_count += block->ele_count;
    file_queue->total_fsize += block->file_size;

    if (file_block_index_push(&file_queue->index, block) == INFQ_ERR) {
        infq_pthread_mutex_unlock(&file_queue->mu);
        INFQ_ERROR_LOG("failed to push block to index, prefix: %s, suffix: %d",
                block->file_prefix,
                block->suffix);
        return INFQ_ERR;
    }
    infq_pthread_mutex_unlock(&file_queue->mu);

    return INFQ_OK;
}
