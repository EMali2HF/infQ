/**
 *
 * @file    thread_safe_infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/06 16:59:11
 */

#include <stdlib.h>

#include "thread_safe_infq.h"

struct _ts_infq_t {
    infq_t              *q;
    pthread_mutex_t     mu;
};

ts_infq_t*
ts_infq_init(const char *data_path)
{
    if (data_path == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return NULL;
    }

    ts_infq_t   *q;

    q = (ts_infq_t *)malloc(sizeof(ts_infq_t));
    if (q == NULL) {
        INFQ_ERROR_LOG("failed to alloc mem for thread safe queue");
        return NULL;
    }

    if ((q->q = infq_init(data_path)) == NULL) {
        INFQ_ERROR_LOG("failed to init infq");
        return NULL;
    }

    if (pthread_mutex_init(&q->mu, NULL) != 0) {
        INFQ_ERROR_LOG("failed to init mutex");
        return NULL;
    }

    return q;
}

int32_t
ts_infq_push(ts_infq_t *infq, void *data, int32_t size)
{
    if (infq == NULL || data == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_push(infq->q, data, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to push");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

int32_t
ts_infq_pop(ts_infq_t *infq, void *buf, int32_t buf_size, int32_t *size)
{
    if (infq == NULL || buf == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_pop(infq->q, buf, buf_size, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
ts_infq_at(ts_infq_t *infq, int32_t idx, void *buf, int32_t buf_size, int32_t *size)
{
    if (infq == NULL || buf == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_at(infq->q, idx, buf, buf_size, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to at");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

int32_t
ts_infq_top(ts_infq_t *infq, void *buf, int32_t buf_size, int32_t *size)
{
    if (infq == NULL || buf == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_top(infq->q, buf, buf_size, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to top");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

int32_t
ts_infq_just_pop(ts_infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_just_pop(infq->q) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to just pop");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
ts_infq_size(ts_infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return -1;
    }

    return infq_size(infq->q);
}

int32_t
ts_infq_msize(ts_infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return -1;
    }

    return infq_msize(infq->q);
}

int32_t
ts_infq_fsize(ts_infq_t *infq)
{
    if (infq == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return -1;
    }

    return infq_fsize(infq->q);
}

void ts_infq_destroy(ts_infq_t *infq)
{
    if (infq == NULL) {
        return;
    }

    infq_destroy(infq->q);
    free(infq);
}

int32_t
ts_infq_pop_zero_cp(ts_infq_t *infq, const void **dataptr, int32_t *size)
{
    if (infq == NULL || dataptr == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_pop_zero_cp(infq->q, dataptr, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to pop zc");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

int32_t
ts_infq_top_zero_cp(ts_infq_t *infq, const void **dataptr, int32_t *size)
{
    if (infq == NULL || dataptr == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_top_zero_cp(infq->q, dataptr, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to top zc");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

int32_t
ts_infq_at_zero_cp(ts_infq_t *infq, int32_t idx, const void **dataptr, int32_t *size)
{
    if (infq == NULL || dataptr == NULL || size == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&infq->mu);

    if (infq_at_zero_cp(infq->q, idx, dataptr, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to at zc");
        pthread_mutex_unlock(&infq->mu);

        return INFQ_ERR;
    }

    pthread_mutex_unlock(&infq->mu);

    return INFQ_OK;
}

