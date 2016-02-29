/**
 *
 * @file    file_block
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 15:37:53
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "file_block.h"
#include "utils.h"

char *INFQ_FILE_BLOCK_PREFIX = "file_block";

#define INFQ_META_INFO_LEN      32      // Magic Number(8B) + Version(8B) + Start index(8B) + Element count(8B)
#define INFQ_IO_BUF_UNIT        4096
#define infq_header_len(fb)     (int32_t)(INFQ_META_INFO_LEN + (fb)->offset_array.size * sizeof(uint32_t))
#define infq_offset_empty(fb)   fb->offset_array.offsets == NULL

static int32_t write_header(file_block_t *file_block, mem_block_t *mem_block);
static int32_t write_data(file_block_t *file_block, mem_block_t *mem_block);

int32_t
file_block_init(file_block_t *file_block, const char *file_path, const char *file_prefix)
{
    if (file_block == NULL || file_path == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    memset(file_block, 0, sizeof(file_block_t));
    file_block->fd = INFQ_UNDEF;
    file_block->start_index = INFQ_UNDEF;
    file_block->suffix = INFQ_UNDEF;

    file_block->file_path = file_path;
    if (file_prefix != NULL) {
        file_block->file_prefix = file_prefix;
    } else {
        file_block->file_prefix = INFQ_FILE_BLOCK_PREFIX;
    }

    return INFQ_OK;
}

/**
 * dump memory block to disk, and copy meta data from memory block to file block
 *
 * |-- Header --|-- Data --|-- Signature(20bit) --|
 *
 */
int32_t
file_block_write(file_block_t *file_block, int32_t suffix, mem_block_t *mem_block)
{
    if (file_block == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    // open file
    char        buf[INFQ_MAX_BUF_SIZE];
    char        meta_buf[16];

    file_block->suffix = suffix;
    // full file path
    if (gen_file_path(
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix,
                buf, INFQ_MAX_BUF_SIZE) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to generate file path, path: %s, prefix: %s, suffix: %lld",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    // fetch signature
    if (mem_block_signature(mem_block, file_block->signature) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to fetch signature");
        return INFQ_ERR;
    }

    file_block->fd = open(buf, O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (file_block->fd == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to open file, file path: %s", buf);
        return INFQ_ERR;
    }

    // meta data, magic number and version
    strcpy(meta_buf, INFQ_MAGIC_NUMBER);
    strcpy(meta_buf + 8, INFQ_VERSION);

    // write magic number and version
    if (infq_pwrite(file_block->fd, meta_buf, sizeof(meta_buf), file_block->file_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to write meta info, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        goto failed;
    }
    file_block->file_size += sizeof(meta_buf);

    // write header
    if (write_header(file_block, mem_block) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to write header");
        goto failed;
    }

    // write data
    if (write_data(file_block, mem_block) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to write data");
        goto failed;
    }

    // write signature
    if (infq_pwrite(
                file_block->fd,
                file_block->signature,
                INFQ_SIGNATURE_LEN,
                file_block->file_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to write signature");
        goto failed;
    }
    file_block->file_size += INFQ_SIGNATURE_LEN;

    return INFQ_OK;

failed:
    if (close(file_block->fd) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to close file, path: %s", buf);
    }

    // remove temp file
    if (unlink(buf) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to remove file, path: %s", buf);
    }

    return INFQ_ERR;
}

/**
 *  |---------------|
 *  | magic number  | 8bytes
 *  |---------------|
 *  | version       | 8bytes
 *  |---------------|
 *  | start index   | 8bytes
 *  |---------------|
 *  | element count | 8bytes    = offset array size
 *  |---------------|
 *  | offset 0      |
 *  |---------------|
 *  | ......        |
 *  |---------------|
 *  | offset n      |
 *  |---------------|
 */
int32_t
file_block_load_header(file_block_t *file_block)
{
    if (file_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char            buf[INFQ_IO_BUF_UNIT];
    int32_t         rlen, *offset;
    int64_t         *meta_array;
    struct stat     finfo;

    // open file when file isn't opened
    if (file_block->fd == INFQ_UNDEF) {
        if (gen_file_path(
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix,
                buf,
                INFQ_IO_BUF_UNIT)) {
            INFQ_ERROR_LOG("failed to generate file path, path: %s, prefix: %s, suffix: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix);
            return INFQ_ERR;
        }

        file_block->fd = open(buf, O_RDONLY);
        if (file_block->fd == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to open file, file path: %s", buf);
            return INFQ_ERR;
        }
    }

    // read file size
    if (fstat(file_block->fd, &finfo) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to fstat, file path: %s", buf);
        return INFQ_ERR;
    }
    file_block->file_size = finfo.st_size;

    // read meta info
    if (infq_read(file_block->fd, buf, INFQ_META_INFO_LEN) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to read meta info, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    // check magic number
    if (strncmp(buf, INFQ_MAGIC_NUMBER, 8) != 0) {
        INFQ_ERROR_LOG("file format is invalid, failed to check magic number, "
                "path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    meta_array = (int64_t *)(buf + 16);
    file_block->start_index = meta_array[0];
    file_block->ele_count = (int32_t)meta_array[1];

    // read offset array
    if (infq_offset_empty(file_block)) {
        if (offset_array_init(&file_block->offset_array) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to init offset array, path: %s, suffix: %d",
                    file_block->file_path, file_block->suffix);
            return INFQ_ERR;
        }
    }

    for (int rest = file_block->ele_count * sizeof(int32_t); rest > 0; rest -= rlen) {
        rlen = rest > INFQ_IO_BUF_UNIT ? INFQ_IO_BUF_UNIT : rest;
        if (infq_read(file_block->fd, buf, rlen) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to read offset array, path: %s, prefix: %s, suffix: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix);
            goto failed;
        }

        for (int j = 0; j < rlen; j += sizeof(int32_t)) {
            offset = (int32_t *)(buf + j);
            if (offset_array_push(&file_block->offset_array, *offset) == INFQ_ERR) {
                INFQ_ERROR_LOG("failed to push offset");
                goto failed;
            }
        }
    }

    INFQ_INFO_LOG("successful to load header, path: %s, prefix: %s, suffix: %d, start index: %lld"
            ", ele count: %d",
            file_block->file_path,
            file_block->file_prefix,
            file_block->suffix,
            file_block->start_index,
            file_block->ele_count);

    return INFQ_OK;

failed:
    // reset file block
    // indicates the block loading is failed
    offset_array_destroy(&file_block->offset_array);

    return INFQ_ERR;
}

int32_t
file_block_load(file_block_t *file_block, mem_block_t *mem_block)
{
    if (file_block == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     idx, rlen, total_size;

    // load header if needed
    if (infq_offset_empty(file_block) || file_block->fd == INFQ_UNDEF) {
        if (file_block_load_header(file_block) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to load file block header, path: %s, prefix: %s, suffix: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix);
            return INFQ_ERR;
        }
    }

    // copy meta data
    mem_block->start_index = file_block->start_index;
    mem_block->ele_count = file_block->ele_count;
    if (offset_array_cp(
                &mem_block->offset_array,
                &file_block->offset_array) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to copy offset array");
        return INFQ_ERR;
    }

    // load memory block
    total_size = file_block->file_size - infq_header_len(file_block) - INFQ_SIGNATURE_LEN;
    if (mem_block->mem_size < total_size) {
        INFQ_ERROR_LOG("mem block isn't big enough, path: %s, prefix: %s, suffix: %d,"
                " file size: %d, mem size: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix,
                file_block->file_size,
                mem_block->mem_size);
        return INFQ_ERR;
    }

    // seek to start of data block
    if (lseek(file_block->fd, infq_header_len(file_block), SEEK_SET) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to seek, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    // read data block at 4KB/op
    idx = 0;
    while (idx < total_size) {
        rlen = total_size - idx > INFQ_IO_BUF_UNIT ? INFQ_IO_BUF_UNIT : total_size - idx;
        if (infq_read(file_block->fd, mem_block->mem + idx, rlen) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to read data block, path: %s, prefix: %s, suffix: %d"
                    ", already read: %d, total: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix,
                    idx,
                    total_size);
            return INFQ_ERR;
        }

        idx += rlen;
    }

    // read signature
    if (infq_read(file_block->fd, file_block->signature, INFQ_SIGNATURE_LEN) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to read signature, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    /**
     * NOTICE: To make sure the consistency of offset array and memory block,
     *      [0, last_offset) of the memory block is dumped. There are some unused bytes at
     *      the beginning of the block.
     */
    mem_block->first_offset = mem_block->offset_array.offsets[0];
    mem_block->last_offset = total_size;

    return INFQ_OK;
}

int32_t
file_block_at(
        file_block_t *file_block,
        int64_t global_idx,
        void *buf,
        int32_t buf_size,
        int32_t *sizeptr)
{
    if (file_block == NULL || buf == NULL || sizeptr == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     offset;
    int64_t     local_idx;

#ifdef D_ASSERT
    INFQ_ASSERT(file_block->ele_count == offset_size(&file_block->offset_array),
            "offset and element count of file block aren't matched, count: %d, "
            "offset start: %d, offset end: %d",
            file_block->ele_count,
            file_block->offset_array.start,
            file_block->offset_array.size);
#endif

    // load header if needed
    if (infq_offset_empty(file_block) || file_block->fd == INFQ_UNDEF) {
        if (file_block_load_header(file_block) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to load file block header, path: %s, prefix: %s, suffix: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix);
            return INFQ_ERR;
        }
    }

    // fetch offset of the element
    local_idx = global_idx - file_block->start_index;
    if (local_idx < 0 || local_idx >= file_block->ele_count) {
        INFQ_ERROR_LOG("idx is invalid, idx: %lld, valid index: [%lld, %lld)",
                global_idx,
                file_block->start_index,
                file_block->start_index + file_block->ele_count);
        return INFQ_ERR;
    }

    if (offset_array_get(&file_block->offset_array, local_idx, &offset) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to get offset, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    // seek to the element
    offset += infq_header_len(file_block);
    if (lseek(file_block->fd, offset, SEEK_SET) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to seek, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    // read data size
    if (infq_read(file_block->fd, sizeptr, sizeof(int32_t)) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to read data len");
        return INFQ_ERR;
    }

    if (*sizeptr > buf_size) {
        INFQ_ERROR_LOG("buffer isn't big enough, buf size: %d, need size: %d",
                buf_size,
                *sizeptr);
        return INFQ_ERR;
    }

    // read data
    if (infq_read(file_block->fd, buf, *sizeptr) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to read data");
        return INFQ_ERR;
    }

    return INFQ_OK;
}

void file_block_destroy(file_block_t *file_block)
{
    if (file_block == NULL) {
        return;
    }

    if (file_block->fd != INFQ_UNDEF) {
        if (close(file_block->fd) == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to close file, path: %s, prefix: %s, suffix: %d",
                    file_block->file_path,
                    file_block->file_prefix,
                    file_block->suffix);
        }
        file_block->fd = INFQ_UNDEF;
    }

    offset_array_destroy(&file_block->offset_array);
}

int32_t file_block_file_delete(file_block_t *file_block)
{
    if (file_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    char    buf[INFQ_MAX_BUF_SIZE];

    if (gen_file_path(
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix,
                buf,
                INFQ_MAX_BUF_SIZE)) {
        INFQ_ERROR_LOG("failed to generate file path");
        return INFQ_ERR;
    }

    // try to close the file
    if (file_block->fd != INFQ_UNDEF) {
        if (close(file_block->fd) == -1) {
            INFQ_ERROR_LOG_BY_ERRNO("failed to close file, file path: %s", buf);
            return INFQ_ERR;
        }
        file_block->fd = INFQ_UNDEF;
    }

    if (unlink(buf) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to unlink file, file path: %s", buf);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

/**
 * | start index | element count | offset array |
 */
int32_t
write_header(file_block_t *file_block, mem_block_t *mem_block)
{
    if (file_block == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int64_t         buf[2];
    int32_t         offset_size, wlen, total_wlen = 0;
    offset_array_t  *offset_array;
    char            *start;

    offset_array = &mem_block->offset_array;
    offset_size = offset_array_size(offset_array);
    INFQ_ASSERT(
            mem_block->ele_count == offset_size,
            "memory block is invalid, ele_count: %d, offset_size: %d, start_index: %lld, "
            "offset start: %d, offset end: %d",
            mem_block->ele_count,
            offset_size,
            mem_block->start_index,
            offset_array->start_idx,
            offset_array->size);

    // collect meta data to buffer, and then write to file
    buf[0] = mem_block->start_index;
    buf[1] = mem_block->ele_count;

    if (infq_pwrite(file_block->fd, buf, sizeof(buf), file_block->file_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to write meta of header");
        return INFQ_ERR;
    }
    file_block->file_size += sizeof(buf);

    // write the offset array to file 4KB/op
    start = (char *)(offset_array->offsets + offset_array->start_idx);
    for (int32_t rest = sizeof(uint32_t) * offset_size; rest > 0; rest -= wlen) {
        wlen = rest > INFQ_IO_BUF_UNIT ? INFQ_IO_BUF_UNIT : rest;
        if (infq_pwrite(
                    file_block->fd,
                    start + total_wlen,
                    wlen,
                    file_block->file_size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to write offset array");
            return INFQ_ERR;
        }
        total_wlen += wlen;
        file_block->file_size += wlen;
    }

    // copy meta data to file block
    file_block->start_index = mem_block->start_index;
    file_block->ele_count = mem_block->ele_count;
    if (offset_array_cp(&file_block->offset_array, offset_array) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to copy offset array to file block, path: %s, suffix: %d",
                file_block->file_path,
                file_block->suffix);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
write_data(file_block_t *file_block, mem_block_t *mem_block)
{
    if (file_block == NULL || mem_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     wlen, total_wlen = 0;

    // copy [0, last_offset) of memory block
    // write memory block data to file 4KB/op
    //
    // Notice: 这里拷贝整个内存块，而不是[first_offset, last_offset)。
    //      这样做的原因是，保证offset array和内存块的一致性。
    //      另一种替代方案是，遍历offset array，对于每个元素都减掉
    //      mem_block->first_offset。这样做比较低效。
    //      在整个infQ中，最多只有push queue和pop queue中，两个被pop
    //      过的memory block会存在浪费。
    for (int32_t rest = mem_block->last_offset; rest > 0; rest -= wlen) {
        wlen = rest > INFQ_IO_BUF_UNIT ? INFQ_IO_BUF_UNIT : rest;
        if (infq_pwrite(
                    file_block->fd,
                    mem_block->mem + total_wlen,
                    wlen,
                    file_block->file_size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to write data, path: %s, suffix: %d");
            return INFQ_ERR;
        }
        total_wlen += wlen;
        file_block->file_size += wlen;
    }

    return INFQ_OK;
}

int32_t
file_block_sync(const file_block_t *file_block)
{
    if (file_block == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    if (file_block->fd == INFQ_UNDEF) {
        INFQ_ERROR_LOG("invalid fd, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    if (fsync(file_block->fd) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to sync data to disk, path: %s, prefix: %s, suffix: %d",
                file_block->file_path,
                file_block->file_prefix,
                file_block->suffix);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

int32_t
file_block_debug_info(const file_block_t *file_block, char *buf, int32_t size)
{
    if (buf == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int32_t     ret;

    if (file_block == NULL) {
        buf[0] = '\0';
    } else {
        ret = snprintf(buf, size,
                "      start_index: %lld,\r\n"
                "      suffix: %d,\r\n"
                "      ele_count: %d,\r\n"
                "      file_prefix: %s,\r\n"
                "      file_size: %d,\r\n",
                file_block->start_index,
                file_block->suffix,
                file_block->ele_count,
                file_block->file_prefix,
                file_block->file_size);
        if (ret == -1 || ret >= size) {
            INFQ_ERROR_LOG("buf is too small, buf size: %d, expected: %d",
                    size, ret);
            return INFQ_ERR;
        }
    }

    return INFQ_OK;
}

int32_t
file_fetch_signature(const char *file_path, unsigned char digest[20])
{
    if (file_path == NULL || digest == NULL) {
        INFQ_ERROR_LOG("invalid param");
        return INFQ_ERR;
    }

    int             fd;
    struct stat     finfo;

    if ((fd = open(file_path, O_RDONLY)) == -1 || fstat(fd, &finfo) == -1) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to open and stat file, path: %s", file_path);
        return INFQ_ERR;
    }

    if (infq_pread(fd, digest, INFQ_SIGNATURE_LEN, finfo.st_size - INFQ_SIGNATURE_LEN) == INFQ_ERR) {
        INFQ_ERROR_LOG_BY_ERRNO("failed to read file, path: %s", file_path);
        return INFQ_ERR;
    }

    return INFQ_OK;
}

void
fetch_readable_signatrue(unsigned char digest[20], char *buf, int32_t len)
{
    for (int i = 0; i < 20; i += 2) {
        snprintf(buf + i, len - i, "%02x%02x", digest[i], digest[i + 1]);
    }
}
