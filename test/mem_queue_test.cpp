/**
 *
 * @file    mem_queue_test
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/13 18:21:03
 */

#include <gtest/gtest.h>
#include <string.h>

#include "mem_queue.h"

#define ERR     -1
#define OK      0

class MemQueueTest: public testing::Test {
protected:
    MemQueueTest() {}
    virtual ~MemQueueTest() {}

    virtual void SetUp() {
        ASSERT_EQ(mem_queue_init(&mem_queue, 3), OK);
        ASSERT_EQ(mem_queue.first_block, -1);
        ASSERT_EQ(mem_queue.last_block, -1);
        ASSERT_TRUE(mem_queue.blocks != NULL);
        ASSERT_EQ(mem_queue.block_num, 3);

        id = 0;
    }

    virtual void TearDown() {
        mem_queue_destroy(&mem_queue);
    }

    mem_queue_t     mem_queue;
    int             id;
};

TEST_F(MemQueueTest, push_err_invalid_param)
{
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, NULL, 0), ERR);
}

TEST_F(MemQueueTest, push_err_block_null)
{
    int     v = 10;

    mem_queue.blocks[0] = NULL;
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), ERR);
}

TEST_F(MemQueueTest, push_err_block_full)
{
    int     full_block_size = 10 * 1024 * 1024;
    char    *data = new char[full_block_size - 100];

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size - 100), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size - 100), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size - 100), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size - 100), ERR);

    EXPECT_EQ(mem_queue.first_block, mem_queue.last_block);
}

TEST_F(MemQueueTest, push_ok1)
{
    int     v = 10;

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    EXPECT_EQ(mem_queue.first_block, 0);
    EXPECT_EQ(mem_queue.last_block, 0);
    EXPECT_EQ(mem_queue.blocks[mem_queue.last_block]->ele_count, 1);
    mem_block_t *mem_block = mem_queue.blocks[mem_queue.last_block];
    EXPECT_EQ(mem_block->start_index, 0);
}

TEST_F(MemQueueTest, push_ok2)
{
    int     v = 10;

    // data + size header(4B)
    int     full_block_size = 10 * 1024 * 1024 - 5;
    char    *data = new char[full_block_size];

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    EXPECT_EQ(mem_queue.first_block, 0);
    EXPECT_EQ(mem_queue.last_block, 1);

    mem_block_t *block = mem_queue.blocks[mem_queue.last_block];
    EXPECT_EQ(block->start_index, id - 1);
}

TEST_F(MemQueueTest, pop_err_invalid_param)
{
    ASSERT_EQ(mem_queue_pop(&mem_queue, NULL, 0, NULL), ERR);
}

TEST_F(MemQueueTest, pop_err_no_enough_mem)
{
    int     v = 10;
    int     data, size;

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_queue_pop(&mem_queue, &data, sizeof(data) - 1, &size), ERR);
    EXPECT_EQ(size, sizeof(v));
}

TEST_F(MemQueueTest, pop_ok_empty)
{
    int     data, size;

    ASSERT_EQ(mem_queue_pop(&mem_queue, &data, sizeof(data), &size), OK);
    EXPECT_EQ(size, 0);
}

TEST_F(MemQueueTest, pop_ok_single_element)
{
    int     v = 10;
    int     data, size;

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_queue_pop(&mem_queue, &data, sizeof(data), &size), OK);
    EXPECT_EQ(size, sizeof(v));
    EXPECT_EQ(data, v);
}

TEST_F(MemQueueTest, pop_ok_mult_elements)
{
    int     a[] = {2, 5, 9, 11};
    int     data, size;

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_queue_push(&mem_queue, id++, &a[i], sizeof(a[i])), OK);
    }
    EXPECT_EQ(mem_queue.ele_count, 4);
    EXPECT_EQ(mem_queue.min_idx, 0);
    EXPECT_EQ(mem_queue.max_idx, id - 1);

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_queue_pop(&mem_queue, &data, sizeof(data), &size), OK);
        EXPECT_EQ(data, a[i]);
    }
    EXPECT_EQ(mem_queue.ele_count, 0);
}

TEST_F(MemQueueTest, pop_ok_multi_blocks)
{
    int     v = 10;
    int     full_block_size = 10 * 1024 * 1024 - sizeof(int) - 1;
    char    *expect = new char[full_block_size];

    char    *data = new char[full_block_size];
    int     data_int, size;

    // fill out the expect buffer
    for (int i = 0; i < full_block_size; i++) {
        expect[i] = i % 255;
    }

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, expect, full_block_size), OK);
    EXPECT_EQ(mem_queue.last_block, 1);

    ASSERT_EQ(mem_queue_pop(&mem_queue, &data_int, sizeof(data), &size), OK);
    EXPECT_EQ(size, sizeof(v));
    EXPECT_EQ(data_int, v);

    ASSERT_EQ(mem_queue_pop(&mem_queue, data, full_block_size, &size), OK);
    EXPECT_EQ(size, full_block_size);
    EXPECT_TRUE(memcmp(data, expect, full_block_size) == 0);
}

TEST_F(MemQueueTest, at_err_invalid_param)
{
    ASSERT_EQ(mem_queue_at(&mem_queue, 0, NULL, 0, NULL), ERR);
}

TEST_F(MemQueueTest, at_err_invalid_idx)
{
    int     v = 10;
    int     data, size;

    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_queue_at(&mem_queue, 1, &data, sizeof(data), &size), ERR);
}

TEST_F(MemQueueTest, at_ok_empty)
{
    int     data, size;

    ASSERT_EQ(mem_queue_at(&mem_queue, 0, &data, sizeof(data), &size), OK);
}

TEST_F(MemQueueTest, at_ok_multi_block)
{
    int     a[] = {2, 4, 6};
    int     full_block_size = 10 * 1024 * 1024 - sizeof(int) * 3;
    char    *data = new char[full_block_size];
    int     dataint, size;

    // first block
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &a[0], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size), OK);
    // second block
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &a[1], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size), OK);
    // third block
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, &a[2], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, id++, data, full_block_size), OK);

    ASSERT_EQ(mem_queue.first_block, 0);
    ASSERT_EQ(mem_queue.last_block, 2);

    ASSERT_EQ(mem_queue.blocks[0]->start_index, 0);
    ASSERT_EQ(mem_queue.blocks[1]->start_index, 2);
    ASSERT_EQ(mem_queue.blocks[2]->start_index, 4);

    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(mem_queue_at(&mem_queue, i * 2, &dataint, sizeof(dataint), &size), OK);
        EXPECT_EQ(size, sizeof(int));
        EXPECT_EQ(a[i], dataint);
    }
}

// different start index
TEST_F(MemQueueTest, at_ok_multi_block_start_index)
{
    int     a[] = {2, 4, 6};
    int     full_block_size = 10 * 1024 * 1024 - sizeof(int) * 3;
    char    *data = new char[full_block_size];
    int     dataint, size;
    int     idx = 111;

    // first block
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, &a[0], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, data, full_block_size), OK);
    // second block
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, &a[1], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, data, full_block_size), OK);
    // third block
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, &a[2], sizeof(int)), OK);
    ASSERT_EQ(mem_queue_push(&mem_queue, idx++, data, full_block_size), OK);

    ASSERT_EQ(mem_queue.first_block, 0);
    ASSERT_EQ(mem_queue.last_block, 2);

    ASSERT_EQ(mem_queue.min_idx, 111);
    ASSERT_EQ(mem_queue.max_idx, 116);

    ASSERT_EQ(mem_queue.blocks[0]->start_index, 111);
    ASSERT_EQ(mem_queue.blocks[1]->start_index, 113);
    ASSERT_EQ(mem_queue.blocks[2]->start_index, 115);

    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(mem_queue_at(&mem_queue, i * 2 + 111, &dataint, sizeof(dataint), &size), OK);
        EXPECT_EQ(size, sizeof(int));
        EXPECT_EQ(a[i], dataint);
    }
}


int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
