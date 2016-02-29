/**
 *
 * @file    utils
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/04 17:18:33
 */

#ifndef COM_MOMO_INFQ_UTILS_H
#define COM_MOMO_INFQ_UTILS_H

#include <stdint.h>
#include <pthread.h>
#include <errno.h>

#include "infq.h"
#include "logging.h"

#define infq_pthread_mutex_lock(mu) \
    do {    \
        switch (pthread_mutex_lock(mu)) {   \
        case EINVAL:    \
            INFQ_ERROR_LOG("mutex is invalid"); \
            break;  \
        case EDEADLK:   \
            INFQ_ERROR_LOG("deadlock"); \
            break;  \
        }   \
    } while (0)

#define infq_pthread_mutex_unlock(mu)   \
    do {    \
        switch (pthread_mutex_unlock(mu)) {  \
        case EINVAL:    \
            INFQ_ERROR_LOG("mutex is invalid"); \
            break;  \
        case EPERM: \
            INFQ_ERROR_LOG("current thread does not hold the lock"); \
            break;  \
        }   \
    } while (0)

int32_t infq_write(int32_t fd, const void *buf, int32_t size);
int32_t infq_read(int32_t fd, void *buf, int32_t rlen);
int32_t infq_pwrite(int32_t fd, const void *buf, int32_t size, int32_t offset);
int32_t infq_pread(int32_t fd, void *buf, int32_t rlen, int32_t offset);

long long time_us();

int32_t make_sure_data_path(const char *data_path);

int32_t gen_file_path(
        const char *file_path,
        const char *file_prefix,
        int32_t suffix,
        char *buf,
        int32_t size);

void to_rm_files_range(
        const file_suffix_range *old,
        const file_suffix_range *new,
        file_suffix_range *to_rm);

#endif
