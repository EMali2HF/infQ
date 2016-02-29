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
main()
{
    infq_t          *q;
    char            buf[1024];
    int             dump_size;
    FILE            *q_org, *q_load;
    int             *data, size;
    int             c = 0;
    infq_config_t   conf = {
        "./persist_test_data",
        1024,
        30,
        30,
        0.4
    };

    q = infq_init_by_conf(&conf, "test");
    if (q == NULL) {
        INFQ_ERROR_LOG("failed to init q by conf");
        return 1;
    }

    q_load = fopen("./queue_load", "r");
    if (q_load == NULL) {
        INFQ_ERROR_LOG("failed to open load file");
        return 1;
    }

    fread(&dump_size, sizeof(dump_size), 1, q_load);
    fread(buf, dump_size, 1, q_load);

    if (infq_load(q, buf, dump_size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to load infq");
        return 1;
    }

    q_org = fopen("./queue_load.data", "w");
    if (q_org == NULL) {
        INFQ_ERROR_LOG("failed to open org file");
        return 1;
    }

    while (infq_size(q) > 0) {
        if (infq_pop_zero_cp(q, (const void **)&data, &size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to pop, idx: %d", c);
        }
        if (size == 0) {
            INFQ_ERROR_LOG("no data, infq size: %d", infq_size(q));
            return 1;
        }
        fprintf(q_org, "%d\n", *data);
        c++;
    }
    fclose(q_org);
    INFQ_INFO_LOG("done load");
}
