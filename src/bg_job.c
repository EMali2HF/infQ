/**
 *
 * @file    bg_job
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/28 11:39:36
 */

#include <string.h>
#include <stdlib.h>

#include "bg_job.h"
#include "utils.h"

// (us)
#define INFQ_LOG_THRESHOLD      10000

void* bg_exec_run(void *);

int32_t
bg_exec_init(bg_exec_t *exec, const char *name)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_attr_t      attr;
    int32_t             err;

    memset(exec, 0, sizeof(bg_exec_t));
    if (strlen(name) > INFQ_MAX_PATH_SIZE - 1) {
        INFQ_ERROR_LOG("name too long, %d chars at most, name: %s",
                INFQ_MAX_PATH_SIZE - 1,
                name);
        return INFQ_ERR;
    }
    strcpy(exec->name, name);

    if ((err = pthread_mutex_init(&exec->mu, NULL)) != 0) {
        INFQ_ERROR_LOG("failed to init mutex, err: %s", strerror(err));
        goto failed;
    }
    if ((err = pthread_cond_init(&exec->cond, NULL)) != 0) {
        INFQ_ERROR_LOG("failed to init conditin variable, err: %s", strerror(err));
        goto failed;
    }

    // use to init thread
    if ((err = pthread_attr_init(&attr)) != 0) {
        INFQ_ERROR_LOG("failed to init mutex attr, err: %s", strerror(err));
        goto failed;
    }

    if ((err = pthread_create(&exec->tid, &attr, bg_exec_run, exec)) != 0) {
        INFQ_ERROR_LOG("failed to create thread, err: %s", strerror(err));
        goto failed;
    }

    if ((err = pthread_attr_destroy(&attr) != 0) && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destory attr, err: %s", strerror(err));
        goto failed;
    }

    INFQ_DEBUG_LOG("successful to init backgroud executor, tid: %d", (int32_t)exec->tid);

    return INFQ_OK;

failed:
    bg_exec_destroy(exec);
    if ((err = pthread_attr_destroy(&attr) != 0) && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destory attr, err: %s", strerror(err));
    }
    return INFQ_ERR;
}

int32_t
bg_exec_stop(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    pthread_mutex_lock(&exec->mu);
    exec->stopped = 1;
    pthread_mutex_unlock(&exec->mu);
    // notify the thread to exit
    pthread_cond_signal(&exec->cond);
    return INFQ_OK;
}

int32_t
bg_exec_suspend(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }
    pthread_mutex_lock(&exec->mu);
    exec->suspended = 1;
    pthread_mutex_unlock(&exec->mu);
    return INFQ_OK;
}

int32_t
bg_exec_continue(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }
    pthread_mutex_lock(&exec->mu);
    exec->suspended = 0;
    pthread_mutex_unlock(&exec->mu);
    pthread_cond_signal(&exec->cond);
    return INFQ_OK;
}

int32_t
bg_exec_continue_if_suspended(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }
    pthread_mutex_lock(&exec->mu);
    if (exec->suspended == 1) {
        INFQ_INFO_LOG("continue a suspended bg executor, name: %s", exec->name);
        exec->suspended = 0;
    }
    pthread_mutex_unlock(&exec->mu);
    pthread_cond_signal(&exec->cond);
    return INFQ_OK;
}

int32_t
bg_exec_add_job(
        bg_exec_t *exec,
        runnable_t runnable,
        void *arg,
        destroy_t destroy,
        tostr_t tostr)
{
    if (exec == NULL || runnable == NULL || arg == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    bg_job_t    *job;

    job = (bg_job_t *)malloc(sizeof(bg_job_t));
    if (job == NULL) {
        INFQ_ERROR_LOG("failed to alloc mem for job");
        return INFQ_ERR;
    }
    job->destory = destroy;
    job->runnable = runnable;
    job->arg = arg;
    job->next = NULL;
    job->tostr = tostr;

    pthread_mutex_lock(&exec->mu);
    // append to job list
    if (exec->jobs_head == NULL || exec->jobs_tail == NULL) {
        exec->jobs_head = exec->jobs_tail = job;
    } else {
        exec->jobs_tail->next = job;
        exec->jobs_tail = job;
    }
    exec->job_count++;

    pthread_cond_signal(&exec->cond);
    pthread_mutex_unlock(&exec->mu);

    return INFQ_OK;
}

int32_t
bg_exec_destroy(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     err;
    long long   ts;
    bg_job_t    *job;

    if (exec->job_count != 0) {
        INFQ_ERROR_LOG("job list is not empty, name: %s, job count: %d", exec->name, exec->job_count);
    }

    if (!exec->stopped) {
        bg_exec_stop(exec);
        ts = time_us();
        if ((err = pthread_join(exec->tid, NULL)) != 0) {
            INFQ_ERROR_LOG("failed to join thread, err: %s", strerror(err));
            return INFQ_ERR;
        }
        INFQ_INFO_LOG("join thread cost %lld us, name: %s", time_us() - ts, exec->name);

        // clear job queue
        while (exec->jobs_head != NULL) {
            job = exec->jobs_head;
            exec->jobs_head = job->next;
            if (job->destory != NULL) {
                job->destory(job->arg);
            }
            free(job);
        }
        exec->jobs_head = exec->jobs_tail = NULL;
    }

    // NOTICE: 允许重复destroy，在mutex被destroy后，再次destroy会报错invalid
    if ((err = pthread_mutex_destroy(&exec->mu)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destory mutex, err: %s", strerror(err));
        return INFQ_ERR;
    }

    if ((err = pthread_cond_destroy(&exec->cond)) != 0 && err != EINVAL) {
        INFQ_ERROR_LOG("failed to destory condition variable, err: %s", strerror(err));
        return INFQ_ERR;
    }

    return INFQ_OK;
}

void*
bg_exec_run(void *arg)
{
    bg_job_t    *job;
    bg_exec_t   *exec;
    long long   s;

    exec = (bg_exec_t *)arg;
    pthread_mutex_lock(&exec->mu);

    while (!exec->stopped) {
        // fetch job
        if (exec->suspended || exec->jobs_head == NULL) {
            pthread_cond_wait(&exec->cond, &exec->mu);
            continue;
        }

        job = exec->jobs_head;

        pthread_mutex_unlock(&exec->mu);

        // execute job
        s = time_us();
        if (job->runnable(job->arg) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to execute background job, name: %s", exec->name);
        }
        s = time_us() - s;

        if (job->tostr != NULL) {
            if (s > INFQ_LOG_THRESHOLD) {
                char    buf[INFQ_MAX_BUF_SIZE];
                if (job->tostr(job->arg, buf, INFQ_MAX_BUF_SIZE) == INFQ_OK) {
                    INFQ_INFO_LOG("finish job, job info: %s, elapse: %lldms, name: %s",
                            buf,
                            s / 1000,
                            exec->name);
                } else {
                    INFQ_INFO_LOG("finish job, elapse: %lldms, name: %s", s / 1000, exec->name);
                }
            }
        }

        // destory job
        if (job->destory != NULL) {
            job->destory(job->arg);
        }

        pthread_mutex_lock(&exec->mu);
        // remove job
        exec->jobs_head = job->next;
        exec->job_count--;
        free(job);
        if (exec->jobs_head == NULL) {
            exec->jobs_tail = NULL;
        }
    }

    if (exec->stopped) {
        pthread_mutex_unlock(&exec->mu);
    }
    INFQ_INFO_LOG("bg_exec exit, name: %s", exec->name);

    return NULL;
}

int32_t
bg_exec_distinct_job(bg_exec_t *exec, job_dup_check_t dup_checker, void *job, int32_t *is_dup)
{
    if (exec == NULL || dup_checker == NULL || is_dup == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    bg_job_t    *last_job;

    *is_dup = INFQ_FALSE;

    pthread_mutex_lock(&exec->mu);
    last_job = exec->jobs_tail;
    if (last_job != NULL) {
        if (dup_checker(job, last_job->arg)) {
            *is_dup = INFQ_TRUE;
        }
    }
    pthread_mutex_unlock(&exec->mu);

    return INFQ_OK;
}

int32_t
bg_exec_pending_task_num(bg_exec_t *exec)
{
    if (exec == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return 0;
    }

    int32_t     n;

    n = 0;
    pthread_mutex_lock(&exec->mu);
    n = exec->job_count;
    pthread_mutex_unlock(&exec->mu);

    return n;
}
