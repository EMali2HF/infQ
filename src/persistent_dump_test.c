/**
 *
 * @file    persistent_test
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/04/08 12:12:31
 */

#include "infq.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
main(int argc, char* argv[])
{
    int             push_num, pop_num;
    infq_t          *q;
    char            buf[1024];
    int             dump_size;
    FILE            *q_org, *q_load;
    int             *data, size;
    infq_config_t   conf = {
        "./persist_test_data",
        1024,
        30,
        30,
        0.4
    };

    if (argc < 4) {
        INFQ_ERROR_LOG("usage: ./persistent_test PUSH_NUM POP_NUM POP?");
        return 1;
    }

    push_num = atoi(argv[1]);
    pop_num = atoi(argv[2]);

    q = infq_init_by_conf(&conf, "test");
    if (q == NULL) {
        INFQ_ERROR_LOG("failed to init q by conf");
        return 1;
    }

    // push data
    for (int i = 0; i < push_num; i++) {
        if (infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to push, idx: %d", i);
            return 1;
        }
    }

    // pop data
    if (pop_num > infq_size(q)) {
        pop_num = infq_size(q);
    }
    for (int i = 0; i < pop_num; i++) {
        if (infq_just_pop(q) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to pop, idx: %d", i);
            return 1;
        }
    }

    // dump
    if (infq_dump(q, buf, 1024, &dump_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to dump");
        return 1;
    }

    q_load = fopen("./queue_load", "w");
    if (q_load == NULL) {
        INFQ_ERROR_LOG("failed to open load file");
        return 1;
    }

    int n = fwrite((char *)&dump_size, sizeof(dump_size), 1, q_load);
    if (n < 1) {
        INFQ_ERROR_LOG("failed to write");
        return 1;
    }
    n = fwrite(buf, dump_size, 1, q_load);
    if (n < 1) {
        INFQ_ERROR_LOG("failed to fwrite");
        return 1;
    }

    if (strcmp(argv[3], "pop") == 0) {
        q_org = fopen("./queue_org.data", "w");
        if (q_org == NULL) {
            INFQ_ERROR_LOG("failed to open org file");
            return 1;
        }
        while (infq_size(q) > 0) {
            if (infq_pop_zero_cp(q, (const void **)&data, &size) == INFQ_ERR) {
                INFQ_ERROR_LOG("failed to pop");
            }
            fprintf(q_org, "%d\n", *data);
        }
        fclose(q_org);
    }
}
