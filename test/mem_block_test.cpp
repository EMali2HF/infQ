/**
 *
 * @file    mem_block_test
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/12 18:04:47
 */

#include <gtest/gtest.h>

#include "mem_block.h"

#define ERR     -1
#define OK      0

class MemBlockTest: public testing::Test {
protected:
    MemBlockTest() {}
    virtual ~MemBlockTest() {}

    virtual void SetUp() {
        mem_block = mem_block_init();
        mem_block->start_index = 0;
        ASSERT_TRUE(mem_block != NULL);
        ASSERT_EQ(mem_block->last_offset, 0);
        ASSERT_EQ(mem_block->first_offset, 0);
        ASSERT_EQ(mem_block->ele_count, 0);
        ASSERT_EQ(mem_block->start_index, 0);
        ASSERT_TRUE(mem_block->offset_array != NULL);
    }

    virtual void TearDown() {
        mem_block_destroy(mem_block);
    }

    mem_block_t     *mem_block;
};

TEST_F(MemBlockTest, push_err_invalid_param)
{
    EXPECT_EQ(mem_block_push(mem_block, NULL, 0), ERR);
}

TEST_F(MemBlockTest, push_err_no_enough_mem)
{
    EXPECT_EQ(mem_block_push(mem_block, mem_block, 10 * 1024 * 1024 + 10), ERR);
}

TEST_F(MemBlockTest, push_ok1)
{
    int     v = 10;
    int     *intptr;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);

    EXPECT_EQ(mem_block->ele_count, 1);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(v));

    intptr = (int *)(mem_block->mem + sizeof(v));
    EXPECT_EQ(*intptr, v);
}

TEST_F(MemBlockTest, push_ok2)
{
    int     a[] = {2, 5, 7, 9};
    int     *intptr;
    char    *p;

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_block_push(mem_block, a + i, sizeof(int)), OK);
    }
    EXPECT_EQ(mem_block->ele_count, 4);
    ASSERT_EQ(mem_block->first_offset, 0);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(int) * 4);

    ASSERT_EQ(mem_block->offset_array->size, 4);
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(mem_block->offset_array->offsets[i], i * 2 * sizeof(int));
    }

    for (int i = 0; i < 4; i++) {
        p = mem_block->mem + i * 2 * sizeof(int);
        intptr = (int *)(p + sizeof(int));
        EXPECT_EQ(*intptr, a[i]);
    }
}

TEST_F(MemBlockTest, push_padding_ok)
{
    int     v = 10;

    int     data_size = sizeof(v) + 2;
    ASSERT_EQ(mem_block_push(mem_block, &v, data_size), OK);
    ASSERT_TRUE(mem_block->last_offset % 8 == 0);
}

TEST_F(MemBlockTest, pop_err_invalid_param)
{
    ASSERT_EQ(mem_block_pop(mem_block, NULL, 0, NULL), ERR);
}

TEST_F(MemBlockTest, pop_err_no_enough_mem)
{
    int     v = 10;
    int     d, size;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);

    ASSERT_EQ(mem_block_pop(mem_block, &d, sizeof(d) - 1, &size), ERR);
    EXPECT_EQ(size, sizeof(d));
}

TEST_F(MemBlockTest, pop_empty1)
{
    int     d, size;
    ASSERT_EQ(mem_block_pop(mem_block, &d, sizeof(d), &size), OK);

    EXPECT_EQ(size, 0);
}

TEST_F(MemBlockTest, pop_empty2)
{
    int     v = 10;
    int     d, size;

    // push
    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);
    EXPECT_EQ(mem_block->first_offset, 0);
    EXPECT_EQ(mem_block->ele_count, 1);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(int));

    // pop
    ASSERT_EQ(mem_block_pop(mem_block, &d, sizeof(d), &size), OK);
    EXPECT_EQ(d, v);
    EXPECT_EQ(size, sizeof(v));
    EXPECT_EQ(mem_block->ele_count, 0);
    EXPECT_EQ(mem_block->first_offset, mem_block->last_offset);
    EXPECT_EQ(mem_block->first_offset, 2 * sizeof(v));
}

TEST_F(MemBlockTest, pop_ok)
{
    int     a[] = {2, 5, 7, 9};
    int     d, size, counter;

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_block_push(mem_block, a + i, sizeof(int)), OK);
    }
    EXPECT_EQ(mem_block->ele_count, 4);
    ASSERT_EQ(mem_block->first_offset, 0);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(int) * 4);

    counter = 0;
    while (mem_block->ele_count) {
        ASSERT_EQ(mem_block_pop(mem_block, &d, sizeof(d), &size), OK);
        EXPECT_EQ(d, a[counter++]);
        EXPECT_EQ(size, sizeof(int));
        EXPECT_EQ(mem_block->first_offset, counter * 2 * sizeof(int));
    }

    EXPECT_EQ(mem_block->ele_count, 0);
    EXPECT_EQ(mem_block->first_offset, mem_block->last_offset);
}

TEST_F(MemBlockTest, at_err_invalid_param)
{
    ASSERT_EQ(mem_block_at(mem_block, 0, NULL, 0, NULL), ERR);
}

TEST_F(MemBlockTest, at_err_idx_outof_range)
{
    int     d, size;
    ASSERT_EQ(mem_block->ele_count, 0);
    ASSERT_EQ(mem_block->start_index, 0);
    ASSERT_EQ(mem_block_at(mem_block, 1, &d, sizeof(d), &size), ERR);
}

TEST_F(MemBlockTest, at_err_no_enough_mem)
{
    int     v = 10;
    int     d, size;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_block_at(mem_block, 0, &d, sizeof(d) - 1, &size), ERR);
    EXPECT_EQ(size, sizeof(d));
}

TEST_F(MemBlockTest, at_ok1)
{
    int     a[] = {2, 5, 7, 9};
    int     d, size;

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_block_push(mem_block, a + i, sizeof(int)), OK);
    }
    EXPECT_EQ(mem_block->ele_count, 4);
    ASSERT_EQ(mem_block->first_offset, 0);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(int) * 4);

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_block_at(mem_block, i, &d, sizeof(d), &size), OK);
        EXPECT_EQ(size, sizeof(d));
        EXPECT_EQ(d, a[i]);
    }
}

TEST_F(MemBlockTest, at_ok2)
{
    int     a[] = {2, 5, 7, 9};
    int     d, size;

    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(mem_block_push(mem_block, a + i, sizeof(int)), OK);
    }
    EXPECT_EQ(mem_block->ele_count, 4);
    ASSERT_EQ(mem_block->first_offset, 0);
    EXPECT_EQ(mem_block->last_offset, 2 * sizeof(int) * 4);

    mem_block->start_index = 110;
    for (int i = 110; i < 114; i++) {
        ASSERT_EQ(mem_block_at(mem_block, i, &d, sizeof(d), &size), OK);
        EXPECT_EQ(size, sizeof(d));
        EXPECT_EQ(d, a[i - 110]);
    }
}

TEST_F(MemBlockTest, top_err_invalid_param)
{
    ASSERT_EQ(mem_block_top(mem_block, NULL, 0, NULL), ERR);
}

TEST_F(MemBlockTest, top_err_no_enough_mem)
{
    int     v = 10;
    int     data, size;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);

    ASSERT_EQ(mem_block_top(mem_block, &data, sizeof(data) - 1, &size), ERR);
    EXPECT_EQ(size, sizeof(v));
}

TEST_F(MemBlockTest, top_ok_empty)
{
    int     data, size;

    ASSERT_EQ(mem_block_top(mem_block, &data, sizeof(data), &size), OK);
    EXPECT_EQ(size, 0);
}

TEST_F(MemBlockTest, top_ok1)
{
    int     v = 10;
    int     data, size;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_block_top(mem_block, &data, sizeof(data), &size), OK);

    EXPECT_EQ(size, sizeof(v));
    EXPECT_EQ(data, v);
}

TEST_F(MemBlockTest, top_ok2)
{
    int     v = 10, y = 11;
    int     data, size;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_block_push(mem_block, &y, sizeof(y)), OK);

    ASSERT_EQ(mem_block_top(mem_block, &data, sizeof(data), &size), OK);
    EXPECT_EQ(data, v);

    ASSERT_EQ(mem_block_pop(mem_block, &data, sizeof(data), &size), OK);
    EXPECT_EQ(data, v);

    ASSERT_EQ(mem_block_top(mem_block, &data, sizeof(data), &size), OK);
    EXPECT_EQ(data, y);
}

TEST_F(MemBlockTest, top_zero_cp_ok)
{
    int     v = 10, y = 11;
    void    *dataptr;
    int     size, *intptr;

    ASSERT_EQ(mem_block_push(mem_block, &v, sizeof(v)), OK);
    ASSERT_EQ(mem_block_push(mem_block, &y, sizeof(y)), OK);

    ASSERT_EQ(mem_block_top_zero_cp(mem_block, &dataptr, &size), OK);
    EXPECT_EQ(size, sizeof(v));
    intptr = (int *)dataptr;
    EXPECT_EQ(*intptr, v);
}


int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
