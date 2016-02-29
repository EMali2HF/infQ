/**
 *
 * @file    infq_bg_jobs
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/08/19 17:21:36
 */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

#include "infq_bg_jobs.h"
#include "utils.h"

void
job_info_destroy(void *arg)
{
    free(arg);
}

int32_t unlink_job(void *arg)
{
    if (arg == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char                buf[INFQ_MAX_BUF_SIZE];
    unlink_job_t        *job_info = (unlink_job_t *)arg;

    if (gen_file_path(
                job_info->file_path,
                job_info->file_prefix,
                job_info->file_block_no,
                buf,
                INFQ_MAX_BUF_SIZE) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to generate file block file path");
        return INFQ_ERR;
    }

    if (unlink(buf) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to unlink file, file path: %s", buf);
        return INFQ_ERR;
    }
    INFQ_INFO_LOG("successful to unlink file block: %s", buf);

    return INFQ_OK;
}

int32_t
dump_job_dup_checker(void *arg, void *last_job)
{
    if (arg == NULL || last_job == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_FALSE;
    }

    struct dump_job_t  *job = (struct dump_job_t *)arg;
    struct dump_job_t  *last = (struct dump_job_t *)last_job;

    // NOTICE: adjacent job is continuous
    //      e.g. mem queue has 5 blocks
    //              job n - 1  => [3, 1)
    //              job n      => [1, 2)
    if (last->end_block != job->start_block) {
        return INFQ_TRUE;
    }

    return INFQ_FALSE;
}

int32_t
load_job_dup_checker(void *arg, void *last_job)
{
    if (arg == NULL || last_job == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_FALSE;
    }

    struct load_job_t *job = (struct load_job_t *)arg;
    struct load_job_t *last = (struct load_job_t *)last_job;

    if (last->file_end_block != job->file_start_block) {
        return INFQ_TRUE;
    }

    return INFQ_FALSE;
}

int32_t
dump_job_tostr(void *arg, char *buf, int32_t size)
{
    struct dump_job_t   *job;
    int32_t             ret;

    job = (struct dump_job_t *)arg;
    ret = snprintf(buf, size, "dump job{start block: %d, end block: %d}",
            job->start_block,
            job->end_block);
    if (ret == -1 || ret >= size) {
        INFQ_ERROR_LOG("failed to stringlize dump job info");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
load_job_tostr(void *arg, char *buf, int32_t size)
{
    struct load_job_t   *job;
    int32_t             ret;

    job = (struct load_job_t *)arg;
    ret = snprintf(buf, size, "load job{start block: %d, end block: %d}",
            job->file_start_block,
            job->file_end_block);
    if (ret == -1 || ret >= size) {
        INFQ_ERROR_LOG("failed to stringlize load job info");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
unlink_job_tostr(void *arg, char *buf, int32_t size)
{
    unlink_job_t *job;
    int32_t             ret;

    job = (unlink_job_t *)arg;
    ret = snprintf(buf, size, "unlink job{file block suffix: %d}", job->file_block_no);
    if (ret == -1 || ret >= size) {
        INFQ_ERROR_LOG("failed to stringlize unlink job info");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

