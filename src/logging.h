/**
 *
 * @file    logging
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 16:28:44
 */

#ifndef COM_MOMO_INFQ_LOGGING_H
#define COM_MOMO_INFQ_LOGGING_H

#include <stdint.h>
#include <assert.h>

#define INFQ_DEBUG_LEVEL    0
#define INFQ_INFO_LEVEL     1
#define INFQ_ERROR_LEVEL    2

#define INFQ_DEBUG_LOG(fmt, ...) \
    infq_log(INFQ_DEBUG_LEVEL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define INFQ_INFO_LOG(fmt, ...) \
    infq_log(INFQ_INFO_LEVEL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define INFQ_ERROR_LOG(fmt, ...) \
    infq_log(INFQ_ERROR_LEVEL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define INFQ_ERROR_LOG_BY_ERRNO(fmt, ...) \
    infq_errno_log(__FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define INFQ_ASSERT(con, fmt, ...)   \
    if (!(con)) {   \
        infq_assert(__FILE__, __LINE__, #con, fmt, ##__VA_ARGS__);    \
        assert(con);    \
    }   \

typedef void (*infq_log_t)(const char *msg);

typedef struct _logging_t logging_t;

void infq_config_logging(int32_t level, infq_log_t debug, infq_log_t info, infq_log_t error);

void infq_log(int32_t level, const char *file, int32_t lineno, const char *fmt, ...);
void infq_assert(const char *file, int32_t lineno, const char *condition, const char *fmt, ...);
void infq_errno_log(const char *file, int32_t lineno, const char *fmt, ...);

extern logging_t logging;

#endif
