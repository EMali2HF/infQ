/**
 *
 * @file    logging
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 16:33:13
 */

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "logging.h"

#define INFQ_MAX_LOG_LEN        1024
#define INFQ_MAX_ERR_MSG_LEN    1024

int32_t     g_logging_level = INFQ_DEBUG_LEVEL;

void default_debug(const char *msg);
void default_info(const char *msg);
void default_error(const char *msg);

struct _logging_t {
    infq_log_t    debug, info, error;
};

// global variable for logging
logging_t logging = {default_debug, default_info, default_error};

void
infq_config_logging(int32_t level, infq_log_t debug, infq_log_t info, infq_log_t error)
{
    if (debug != NULL) {
        logging.debug = debug;
    }

    if (info != NULL) {
        logging.info = info;
    }

    if (error != NULL) {
        logging.error = error;
    }

    switch (level) {
        case INFQ_DEBUG_LEVEL:
        case INFQ_INFO_LEVEL:
        case INFQ_ERROR_LEVEL:
            g_logging_level = level;
    }
}

void
infq_log(int32_t level, const char *file, int32_t lineno, const char *fmt, ...)
{
    char msg[INFQ_MAX_LOG_LEN];

    int ret;

    if (level < g_logging_level) {
        return;
    }

    // add file name and line no
    ret = snprintf(msg, INFQ_MAX_LOG_LEN, "[%s:%d]  ", file, lineno);
    // NOTICE: write error
    if (ret == -1 || ret >= INFQ_MAX_LOG_LEN) {
        return;
    }

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg + ret - 1, sizeof(msg) - ret + 1, fmt, ap);
    va_end(ap);

    if (level == INFQ_DEBUG_LEVEL) {
        logging.debug(msg);
    } else if (level == INFQ_INFO_LEVEL) {
        logging.info(msg);
    } else if (level == INFQ_ERROR_LEVEL) {
        logging.error(msg);
    }
}

void
infq_assert(const char *file, int32_t lineno, const char *condition, const char *fmt, ...)
{
    char    msg[INFQ_MAX_LOG_LEN];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, INFQ_MAX_LOG_LEN, fmt, ap);
    va_end(ap);

    infq_log(INFQ_ERROR_LEVEL, file, lineno, "assertion error: '%s', msg: %s", condition, msg);
}

void
infq_errno_log(const char *file, int32_t lineno, const char *fmt, ...)
{
    char    msg[INFQ_MAX_LOG_LEN];
    char    errmsg[INFQ_MAX_ERR_MSG_LEN];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, INFQ_MAX_LOG_LEN, fmt, ap);
    va_end(ap);

#ifdef _GNU_SOURCE
    // linux
    infq_log(INFQ_ERROR_LEVEL, file, lineno, "%s, err: %s",
            msg,
            strerror_r(errno, errmsg, INFQ_MAX_ERR_MSG_LEN));
#else
    strerror_r(errno, errmsg, INFQ_MAX_ERR_MSG_LEN);
    infq_log(INFQ_ERROR_LEVEL, file, lineno, "%s, err: %s", msg, errmsg);
#endif
}

void
default_debug(const char *msg)
{
    fprintf(stdout, "%s\n", msg);
}

void
default_info(const char *msg)
{
    fprintf(stdout, "%s\n", msg);
}

void
default_error(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
}
