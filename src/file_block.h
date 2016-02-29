/**
 *
 * @file    file_block
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/11 11:48:38
 */

#ifndef COM_MOMO_INFQ_FILE_BLOCK_H
#define COM_MOMO_INFQ_FILE_BLOCK_H

#include <stdint.h>
#include <stdio.h>

#include "offset_array.h"
#include "mem_block.h"
#include "infq.h"

#define MAX_FILENAME_CHARS      100
#define INFQ_SIGNATURE_LEN      20

extern char *INFQ_FILE_BLOCK_PREFXI;

typedef struct _file_block_t {
    int64_t                 start_index;        /* Global index of the first element */
    volatile int32_t        ele_count;          /* Element count */
    const char              *file_path;         /* A char pointer points to the path which the files
                                                   of InfQ locate.
                                                   Point to file_queue_t.file_path */
    const char              *file_prefix;       /* Prefix of the file's name of the file block.
                                                   Such as 'file_block_' or 'pop_block_' */
    int32_t                 suffix;             /* Suffix of the file's name of the file block.
                                                   It's a number sequence. */
    int32_t                 fd;                 /* File descriptor of the file block.
                                                   If the file isn't opened, it's -1. */
    int32_t                 file_size;          /* Size of the file */
    offset_array_t          offset_array;       /* Mapping the offset of element by index */
    struct _file_block_t    *next;              /* All the blocks in a file queue are organized into a
                                                   linked-list. 'next' is a pointer points to the next block */
    unsigned char           signature[INFQ_SIGNATURE_LEN];  /* The signature of the content, used to match the
                                                               content of memory block and file block */
} file_block_t;

/**
 * @brief Init the file block.
 * @param file_path: The directory which the file block locates
 * @param file_prefix: The prefix of the file which the file block correspond with.
 *          There are two type prefix:
 *          - file block in file queue. This is the default value when NULL is passed.
 *          - file block in pop queue when infQ dumped.
 */
int32_t file_block_init(file_block_t *file_block, const char *file_path, const char *file_prefix);

int32_t file_block_write(file_block_t *file_block, int32_t suffix, mem_block_t *mem_block);
int32_t file_block_load_header(file_block_t *file_block);
int32_t file_block_load(file_block_t *file_block, mem_block_t *mem_block);
int32_t file_block_at(
        file_block_t *file_block,
        int64_t global_idx,
        void *buf,
        int32_t buf_size,
        int32_t *sizeptr);
void file_block_destroy(file_block_t *file_block);
int32_t file_block_file_delete(file_block_t *file_block);
int32_t file_block_sync(const file_block_t *file_block);
int32_t file_block_debug_info(const file_block_t *file_block, char *buf, int32_t size);
int32_t file_fetch_signature(const char *file_path, unsigned char digest[20]);
void fetch_readable_signatrue(unsigned char digest[20], char *buf, int32_t len);

#endif
