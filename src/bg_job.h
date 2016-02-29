/**
 *
 * @file    bg_job
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/26 17:36:55
 */

#ifndef COM_MOMO_INFQ_BG_JOB_H
#define COM_MOMO_INFQ_BG_JOB_H

#include <pthread.h>

#include "infq.h"

typedef int32_t (*runnable_t)(void *arg);
typedef void (*destroy_t)(void *arg);
typedef int32_t (*tostr_t)(void *arg, char *buf, int32_t size);
typedef int32_t (job_dup_check_t)(void *arg, void *last_job);

typedef struct _bg_job_t {
    runnable_t          runnable;
    void                *arg;
    struct _bg_job_t    *next;
    destroy_t           destory;
    tostr_t             tostr;
} bg_job_t;

typedef struct _bg_exec_t {
    pthread_t           tid;
    char                name[INFQ_MAX_PATH_SIZE];

    // used to protect the job list
    pthread_mutex_t     mu;
    pthread_cond_t      cond;

    bg_job_t            *jobs_head, *jobs_tail;
    volatile int32_t    job_count;
    volatile int8_t     stopped;
    volatile int8_t     suspended;
} bg_exec_t;

int32_t bg_exec_init(bg_exec_t *exec, const char *name);
int32_t bg_exec_start(bg_exec_t *exec);
int32_t bg_exec_stop(bg_exec_t *exec);
int32_t bg_exec_suspend(bg_exec_t *exec);
int32_t bg_exec_continue(bg_exec_t *exec);
int32_t bg_exec_continue_if_suspended(bg_exec_t *exec);
int32_t bg_exec_destroy(bg_exec_t *exec);

int32_t bg_exec_add_job(bg_exec_t *exec, runnable_t runnable, void *arg, destroy_t destory, tostr_t tostr);
int32_t bg_exec_distinct_job(bg_exec_t *exec, job_dup_check_t distinctor, void *job, int32_t *is_dup);
int32_t bg_exec_pending_task_num(bg_exec_t *exec);

#endif
