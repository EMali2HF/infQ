/**
 *
 * @file    multi_test
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/03/06 17:35:30
 */

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "thread_safe_infq.h"

ts_infq_t   *q;

void* push_run(void *);
void* pop_run(void *);

int
main(int argc, char* argv[]) {
    pthread_t   t1, t2;
    int         push_sleep, pop_sleep;

    if (argc < 3) {
        INFQ_ERROR_LOG("USAGE: multi_test push_sleep pop_sleep");
        return -1;
    }

    push_sleep = atoi(argv[1]);
    pop_sleep = atoi(argv[2]);

    q = ts_infq_init("./data");
    if (q == NULL) {
        INFQ_ERROR_LOG("failed to create infq");
        return -1;
    }

    pthread_create(&t1, NULL, push_run, &push_sleep);
    pthread_create(&t2, NULL, pop_run, &pop_sleep);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
}

void*
push_run(void *a)
{
    int     *sleep_s;
    int     n;

    sleep_s = (int *)a;
    n = *sleep_s * 100;

    for (int i = 0; i < 100 * 300; i++) {
        if (ts_infq_push(q, &i, sizeof(i)) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to push %d", i);
        }

        printf("push => %d\n", i);
        if (i % n == 0) {
            sleep(1);
        }
    }

    return NULL;
}

void*
pop_run(void *a)
{
    int     num, size, *sleep_s, u, counter;

    sleep_s = (int *)a;
    u = *sleep_s * 100;
    counter = 0;

    while (1) {
        if (ts_infq_pop(q, &num, sizeof(num), &size) == INFQ_ERR) {
            INFQ_ERROR_LOG("failed to pop");
        }

        if (size == 0) {
            continue;
        }

        if (num == 100 * 300) {
            break;
        }

        printf("pop => %d\n", num);
        counter++;
        if (counter % u == 0) {
            sleep(1);
        }
    }

    return NULL;
}
