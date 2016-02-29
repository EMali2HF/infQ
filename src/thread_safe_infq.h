/**
 *
 * @file    thread_safe_infq
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/06 16:52:45
 */

#ifndef COM_MOMO_INFQ_THREAD_SAFE_INFQ_H
#define COM_MOMO_INFQ_THREAD_SAFE_INFQ_H

#include <pthread.h>

#include "infq.h"

typedef struct _ts_infq_t   ts_infq_t;

ts_infq_t* ts_infq_init(const char *data_path);
int32_t ts_infq_push(ts_infq_t *infq, void *data, int32_t size);
int32_t ts_infq_pop(ts_infq_t *infq, void *buf, int32_t buf_size, int32_t *size);
int32_t ts_infq_at(ts_infq_t *infq, int32_t idx, void *buf, int32_t buf_size, int32_t *size);
int32_t ts_infq_top(ts_infq_t *infq, void *buf, int32_t buf_size, int32_t *size);
int32_t ts_infq_just_pop(ts_infq_t *infq);
int32_t ts_infq_size(ts_infq_t *infq);
int32_t ts_infq_msize(ts_infq_t *infq);
int32_t ts_infq_fsize(ts_infq_t *infq);
void ts_infq_destroy(ts_infq_t *infq);

int32_t ts_infq_pop_zero_cp(ts_infq_t *infq, const void **dataptr, int32_t *size);
int32_t ts_infq_top_zero_cp(ts_infq_t *infq, const void **dataptr, int32_t *size);
int32_t ts_infq_at_zero_cp(ts_infq_t *infq, int32_t idx, const void **dataptr, int32_t *size);


#endif
