/**
 *
 * @file    file_block_reader
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/05/06 14:45:02
 */

#include "file_block.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define MAX_PATH_CHARS  1024

int parse_file_blk_info(const char *fpath, char *dpath_buf, char *prefix_buf, int *suffix);
void print_file_blk_info(file_block_t *fblock);

int
main(int argc, char* argv[])
{
    char            *file_path;
    int             idx, lastarg, i, suffix;
    char            prefix_buf[MAX_PATH_CHARS];
    char            dpath_buf[MAX_PATH_CHARS];
    file_block_t    fblock;
    mem_block_t     *mblock;

    idx = -1;
    file_path = NULL;
    for (i = 1; i < argc; i++) {
        lastarg = (i == argc - 1);
        if (!strcmp(argv[i], "-f")) {
            if (lastarg) goto invalid;
            file_path = argv[i + 1];
            i++;
        } else if (!strcmp(argv[i], "-i")) {
            if (lastarg) goto invalid;
            idx = atoi(argv[i + 1]);
            i++;
        } else {
            goto invalid;
        }
    }

    if (file_path == NULL) {
        printf("file path must specified\n");
        return 1;
    }

    if (parse_file_blk_info(file_path, dpath_buf, prefix_buf, &suffix) == -1) {
        printf("failed to parser file path, path: %s\n", file_path);
        return 1;
    }

    if (file_block_init(&fblock, dpath_buf, prefix_buf) == INFQ_ERR) {
        printf("failed to init file block\n");
        return 1;
    }
    fblock.suffix = suffix;

    if (file_block_load_header(&fblock) == INFQ_ERR) {
        printf("failed to load header\n");
        return 1;
    }

    if (idx != -1) {
        mblock = mem_block_init(fblock.file_size);
        if (mblock == NULL) {
            printf("failed to init mem block\n");
            return 1;
        }
        if (file_block_load(&fblock, mblock) == INFQ_ERR) {
            printf("failed to load block\n");
            return 1;
        }
    }

    print_file_blk_info(&fblock);
    return 0;

invalid:
    printf("Invalid option for \"%s\" or option missing\n", argv[i]);
    return 1;
}

int
parse_file_blk_info(const char *fpath, char *dpath_buf, char *prefix_buf, int *suffix)
{
    if (fpath == NULL) {
        printf("fpath is NULL\n");
        return -1;
    }

    const char    *p, *q;

    p = fpath + strlen(fpath) - 1;
    while (p != NULL && *p != '_') {
        p--;
    }

    if (p == NULL) {
        printf("invalid file path\n");
        return -1;
    }
    *suffix = atoi(p + 1);

    q = p;
    while (q != NULL && *q != '/') {
        q--;
    }

    if (q == NULL) {
        // file in current dir
        strncpy(prefix_buf, fpath, fpath - p);
        prefix_buf[p - q - 1] = 0;
        strcpy(dpath_buf, "./");
    } else {
        strncpy(prefix_buf, q + 1, p - q - 1);
        prefix_buf[p - q - 1] = 0;
        strncpy(dpath_buf, fpath, q - fpath + 1);
        dpath_buf[q - fpath + 1] = 0;
    }

    return 0;
}

void
print_file_blk_info(file_block_t *fblock)
{
    printf("File Block: \r\n"
            "ele_count: %d\r\n"
            "start_index: %lld\r\n"
            "file_size: %d\r\n"
            "prefix: %s\r\n"
            "suffix: %d\r\n",
            fblock->ele_count,
            fblock->start_index,
            fblock->file_size,
            fblock->file_prefix,
            fblock->suffix);
}
