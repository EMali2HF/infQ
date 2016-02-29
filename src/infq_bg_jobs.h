/**
 *
 * @file    infq_bg_jobs
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/08/19 17:19:16
 */

#ifndef COM_MOMO_INFQ_INFQ_BG_JOBS_H
#define COM_MOMO_INFQ_INFQ_BG_JOBS_H

#include <stdint.h>
#include "infq.h"

struct dump_job_t {
    // [start_block, end_block)
    int32_t     start_block;
    int32_t     end_block;
    int32_t     block_num;
    infq_t      *infq;
};

struct load_job_t {
    // file block suffix [start_block, end_block)
    int32_t     file_start_block;
    int32_t     file_end_block;
    infq_t      *infq;
};

typedef struct _unlink_job_t {
    int32_t file_block_no;
    char *file_path;
    char *file_prefix;
} unlink_job_t;

void job_info_destroy(void *);
int32_t unlink_job(void *);

int32_t dump_job_dup_checker(void *arg, void *last_job);
int32_t load_job_dup_checker(void *arg, void *last_job);
int32_t dump_job_tostr(void *arg, char *buf, int32_t size);
int32_t load_job_tostr(void *arg, char *buf, int32_t size);
int32_t unlink_job_tostr(void *arg, char *buf, int32_t size);

#endif
