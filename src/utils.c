/**
 *
 * @file    utils
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/04 17:19:17
 */

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdio.h>

#include "utils.h"
#include "infq.h"

int32_t
infq_write(int32_t fd, const void *buf, int32_t size)
{
    if (fd == -1 || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     wlen;

    wlen = write(fd, buf, size);
    if (wlen == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to write");
        return INFQ_ERR;
    } else if (wlen < size) {
        INFQ_ERROR_LOG("failed to write, partly write, wlen: %d, expect len: %d",
                wlen, size);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_pwrite(int32_t fd, const void *buf, int32_t size, int32_t offset)
{
    if (fd == -1 || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     wlen;

    wlen = pwrite(fd, buf, size, offset);
    if (wlen == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to write");
        return INFQ_ERR;
    } else if (wlen < size) {
        INFQ_ERROR_LOG("failed to write, partly write, wlen: %d, expect len: %d",
                wlen, size);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_read(int32_t fd, void *buf, int32_t rlen)
{
    if (fd == -1 || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int     ret;

    ret = read(fd, buf, rlen);
    if (ret == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to read");
        return INFQ_ERR;
    } else if (ret < rlen) {
        INFQ_ERROR_LOG("failed to read, partly read, read len: %d, expected: %d",
                ret, rlen);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
infq_pread(int32_t fd, void *buf, int32_t rlen, int32_t offset)
{
    if (fd == -1 || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int     ret;

    ret = pread(fd, buf, rlen, offset);
    if (ret == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to read");
        return INFQ_ERR;
    } else if (ret < rlen) {
        INFQ_ERROR_LOG("failed to read, partly read, read len: %d, expected: %d",
                ret, rlen);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

long long
time_us()
{
    struct timeval  tv;
    long long       us;

    gettimeofday(&tv, NULL);

    us = ((long long)tv.tv_sec) * 1000000;
    us += tv.tv_usec;

    return us;
}

int32_t
make_sure_data_path(const char *data_path)
{
    if (data_path == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char            buf[INFQ_MAX_PATH_SIZE];
    int32_t         rtrim;
    char            *p, *q;
    const char      *abspath;
    struct stat     statinfo;

    // convert to absolute path
    if (*data_path != '/') {
        if (getcwd(buf, INFQ_MAX_PATH_SIZE) == NULL) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to get cwd");
            return INFQ_ERR;
        }

        if (strlen(buf) && buf[strlen(buf) - 1] != '/') {
            strcat(buf, "/");
        }

        p = (char *)data_path;
        rtrim = 0;

        // change to upper dir
        while (strlen(p) >= 3 && p[0] == '.' && p[1] == '.' && p[2] == '/') {
            p += 3;
            rtrim++;

            if (strlen(p) > 1) {
                q = buf + strlen(buf) - 2;
                while (*p != '/') {
                    p--;
                    rtrim++;
                }
            }
        }

        // convert ./././aa/bb/c => aa/bb/c
        while (strlen(p) >= 2 && p[0] == '.' && p[1] == '/') {
            p += 2;
        }

        if (strlen(buf) - rtrim + strlen(p) >= INFQ_MAX_PATH_SIZE) {
            INFQ_ERROR_LOG("path buffer is too small, size: %d, expect: %d",
                    INFQ_MAX_PATH_SIZE,
                    strlen(buf) - rtrim + strlen(p));
            return INFQ_ERR;
        }

        strcpy(buf + strlen(buf) - rtrim, p);
        abspath = buf;
    } else {
        abspath = data_path;
    }

    // check existence
    if (access(abspath, F_OK) == -1) {
        INFQ_INFO_LOG("path not exist, create dir, path: %s", abspath);
        if (mkdir(abspath, 0755) == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to create dir, path: %s", abspath);
            return INFQ_ERR;
        }
    } else {
        if (stat(abspath, &statinfo) == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to stat file, path: %s", abspath);
            return INFQ_ERR;
        }

        if (!S_ISDIR(statinfo.st_mode)) {
            INFQ_ERROR_LOG("data path isn't dir");
            return INFQ_ERR;
        }
    }

    return INFQ_OK;
}

int32_t
gen_file_path(
        const char *file_path,
        const char *file_prefix,
        int32_t suffix,
        char *buf,
        int32_t size)
{
    if (file_path == NULL || file_prefix == NULL || buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int ret;

    ret = snprintf(buf, size, "%s/%s_%d", file_path, file_prefix, suffix);
    if (ret < 0) {
        INFQ_ERROR_LOG("failed to snprintf, err: %s", strerror);
        return INFQ_ERR;
    } else if (ret >= size) {
        INFQ_ERROR_LOG("buffer is not enough, buf size: %d, expect: %d", size, ret);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

void to_rm_files_range(
        const file_suffix_range *old,
        const file_suffix_range *new,
        file_suffix_range *to_rm)
{
    if (old == NULL || new == NULL || to_rm == NULL) {
        INFQ_ERROR_LOG("invald param");
        return;
    }

    memset(to_rm, 0, sizeof(file_suffix_range));

    if (new->start == new->end) {
        // 'new->start == new->end' means that the new dump
        // doesn't have files. Just remove files in old dump
        to_rm->start = old->start;
        to_rm->end = old->end;
    } else {
        if (old->start == old->end) {
            // 'old->start == old->end' means that the old dump
            // meta doesn't have files. No need to remove.
            to_rm->start = to_rm->end = INFQ_UNDEF;
            return;
        }

        to_rm->start = old->start;
        to_rm->end = new->start < old->end ? new->start : old->end;
    }
}
