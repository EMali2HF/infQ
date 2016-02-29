/**
 *
 * @file    main
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/03 18:24:29
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#include "infq.h"

void do_pop(infq_t *q, int n);

void at_test(infq_t *q);
void push_pop_test(infq_t *q);
void check_at(infq_t *q);
void dump_test(infq_t *q, int blk_count);
void push_test(infq_t *q);

int
main()
{
    infq_t      *q;
    char        buf[2048];

    infq_config_t conf = {
        "./data",
        1024,
        40,
        30,
        0.4
    };
    /*infq_config_logging(INFQ_DEBUG_LEVEL, NULL, NULL, NULL);*/
    infq_config_logging(INFQ_INFO_LEVEL, NULL, NULL, NULL);
    q = infq_init_by_conf(&conf, "test");
    if (q == NULL) {
        printf("failed to init infq");
    }

    printf("%s\n", infq_debug_info(q, buf, 2048));

    /*at_test(q);*/
    push_test(q);
    /*push_pop_test(q);*/
    /*dump_test(q, 10);*/
    infq_destroy_completely(q);

    return 0;
}

void
do_pop(infq_t *q, int n)
{
    int     *num, size;
    static  int last_val = -1;

    for (int i = 0; i < n; i++) {
        if (infq_pop_zero_cp(q, (const void **)&num, &size) == INFQ_ERR) {
            return;
        }

        if (num == NULL) {
            continue;
        }

        printf("pop => %d\n", *num);
        if (infq_check_popq(q) == INFQ_FALSE) {
            printf("popq check error\n");
            exit(1);
        }

        if (infq_check_pushq(q) == INFQ_FALSE) {
            printf("pushq check error\n");
            exit(1);
        }

        if (last_val + 1 != *num) {
            INFQ_ERROR_LOG("ERR, last: %d, now: %d", last_val, *num);
            exit(1);
        }
        last_val = *num;
    }
}

void at_test(infq_t *q)
{
    const void  *data;
    int         data_size, *intptr;

    for (int i = 0; i < 10000; i++) {
        if (infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to push");
            return;
        }
    }

    for (int i = 0; i < 150; i++) {
        if (infq_at_zero_cp(q, i, &data, &data_size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to at");
            return;
        }

        intptr = (int *)data;
        if (*intptr != i) {
            INFQ_ERROR_LOG("value not equel at %d, val: %d, expected: %d",
                    i, *intptr, i);
            return;
        }
    }

    INFQ_ERROR_LOG("fin at assertion, then pop some data");
    for (int i = 0; i < 100; i++) {
        if (infq_just_pop(q) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to just pop");
            return;
        }
    }

    infq_top_zero_cp(q, &data, &data_size);
    intptr = (int *)data;
    INFQ_ERROR_LOG("top value: %d", *intptr);

    for (int i = 100; i < 150; i++) {
        if (infq_at_zero_cp(q, i, &data, &data_size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to at, idx: %d", i);
            return;
        }

        intptr = (int *)data;
        if (*intptr != i + 100) {
            INFQ_ERROR_LOG("value not equel after pop at %d, val: %d, expected: %d",
                    i, *intptr, i + 100);
            return;
        }
    }
    INFQ_ERROR_LOG("fin");
}

void push_pop_test(infq_t *q)
{
    int     m, n;

    for (int i = 0; i < 50 * 60000 || infq_size(q) != 0; i++) {
        if (i < 50 * 60000) {
            if (infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
                printf("failed to push\n");
                return;
            }

            printf("push => %d, s => %d\n", i, infq_size(q));
            if (i % 500 == 0) {
                sleep(1);
            }
        }

        if (i % 60 == 0) {
            m = i > 40 ? 40 : i - 1;
            if (m > 0) {
                n = rand() % m;
                do_pop(q, n);
            }
        }

        if (i % 33 == 0) {
            check_at(q);
        }
    }

    sleep(3);
    printf("fin sleep");

    int     *data;
    int     size;

    for (int i = 0; i < 4000000; i++) {
        if (infq_pop_zero_cp(q, (const void **)&data, &size) == INFQ_ERR) {
            printf("failed to pop\n");
        }
    }
}

void check_at(infq_t *q)
{
    int     n, m, len;
    int     *data, *top;
    int     data_size;

    len = 150;
    // random access range [n, m], 50 values at most
    n = infq_size(q);
    // start index
    m = rand() % n;
    if (n - m > len) {
        n = m + len;
    }

    infq_top_zero_cp(q, (const void **)&top, &data_size);

    for (int i = m; i < n; i++) {
        if (infq_at_zero_cp(q, i, (const void **)&data, &data_size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to at");
            return;
        }

        if (*data != *top + i) {
            INFQ_ERROR_LOG("at not match, val: %d, expected: %d", *data, *top + i);
            return;
        }
        INFQ_ERROR_LOG("at match, val: %d", *data);
    }
}

void dump_test(infq_t *q, int block_num)
{
    infq_t      *qu;

    qu = infq_init("./data", "test");
    if (qu == NULL) {
        INFQ_ERROR_LOG("failed to init infq");
        return;
    }

    for (int i = 0; i < block_num * 256; i++) {
        if (infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to push");
            return;
        }
        /*INFQ_ERROR_LOG("push => %d", i);*/
    }

    char    buf[1024];
    int     size;

    if (infq_dump(q, buf, 1024, &size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to dump");
        return;
    }

    if (infq_load(qu, buf, size) == INFQ_ERR) {
        INFQ_ERROR_LOG("failed to load");
        return;
    }

    if (infq_size(q) != infq_size(qu)) {
        INFQ_ERROR_LOG("ele count not match, org: %d, load: %d",
                infq_size(q), infq_size(qu));
        return;
    }

    INFQ_INFO_LOG("queue size: %d", infq_size(q));

    int     *n, *m;
    int     s1, s2;
    int     i = 0;
    while (infq_size(q) > 0) {
        if (infq_pop_zero_cp(q, (const void **)&n, &s1) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to pop, idx: %d", i);
            return;
        }
        if (infq_pop_zero_cp(qu, (const void **)&m, &s2) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to pop qu, idx: %d", i);
            return;
        }

        if (*n != *m) {
            INFQ_ERROR_LOG("value not match, idx: %d, n: %d, m: %d",
                    i, *n, *m);
            return;
        }
        i++;
    }
    INFQ_INFO_LOG("fin match");
}

void push_test(infq_t *q)
{
    int     i = 0;
    int     failed = 0;
    int     *num, size;
    static int last_val = -1;

    while (i < 50 * 600) {
        if (infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
            failed++;
            if (failed % 100 == 0) {
                sleep(1);
            }
            continue;
        }
        failed = 0;

        if (i % 4000 == 0) {
            sleep(1);
        }
        i++;
    }

    printf("start pop\n");
    i = 0;
    while (infq_size(q) > 0) {
        if (infq_pop_zero_cp(q, (const void **)&num, &size) == INFQ_ERR) {
            printf("pop error");
            sleep(1);
        }

        if (last_val + 1 != *num) {
            INFQ_ERROR_LOG("ERR, last: %d, now: %d", last_val, *num);
        }
        last_val = *num;

        if (i++ % 500 == 0) {
            sleep(1);
        }
    }

    char    buf[2048];

    printf("fin sleep, %s\n", infq_debug_info(q, buf, 2048));
}
