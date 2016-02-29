/**
 *
 * @file    file_block_test
 * @author  chosen0ne(louzhenlin86@126.com)
 * @date    2015/02/15 11:56:45
 */

#include <gtest/gtest.h>
#include <string.h>

#include "file_block.h"

#define ERR     -1
#define OK      0

const char *DEFAULT_FILE_PATH = "./file_blocks";

class FileBlockTest: public testing::Test {
protected:
    FileBlockTest() {}
    ~FileBlockTest() {}

    virtual void SetUp() {
        strcpy(file_path, DEFAULT_FILE_PATH);
        ASSERT_EQ(file_block_init(&file_block, file_path), OK);
        ASSERT_STREQ(DEFAULT_FILE_PATH, file_block.file_path);

        // init mem block
        InitSingleBlk();
    }

    virtual void TearDown() {
        mem_block_destroy(single_mem_block);
        file_block_file_delete(&file_block);
        file_block_destroy(&file_block);
    }

    void InitSingleBlk() {
        int     a[] = {10, 3, 5, 56};

        single_mem_block = mem_block_init();
        single_mem_block->start_index = 0;
        ASSERT_TRUE(single_mem_block != NULL);

        for (int i = 0; i < 4; i++) {
            ASSERT_EQ(mem_block_push(single_mem_block, &a[i], sizeof(int)), OK);
        }
    }

    file_block_t    file_block;
    mem_block_t     *single_mem_block, *multi_mem_block;
    char            file_path[100];
};


TEST_F(FileBlockTest, write_err_invalid_param)
{
    ASSERT_EQ(file_block_write(NULL, 0, NULL), ERR);
}

TEST_F(FileBlockTest, write_ok)
{
    ASSERT_EQ(file_block_write(&file_block, 0, single_mem_block), OK);
}

TEST_F(FileBlockTest, load_ok)
{
    mem_block_t     *block = mem_block_init();
    file_block_t    fblock;
    int             data1, size1;
    int             data2, size2;

    ASSERT_EQ(file_block_write(&file_block, 0, single_mem_block), OK);

    ASSERT_EQ(file_block_init(&fblock, file_path), OK);
    ASSERT_STREQ(fblock.file_path, file_path);
    fblock.suffix = 0;
    ASSERT_EQ(file_block_load(&fblock, block), OK);

    for (int i = 0; i < single_mem_block->ele_count; i++) {
        ASSERT_EQ(mem_block_at(block, i, &data1, sizeof(data1), &size1), OK);
        ASSERT_EQ(mem_block_at(single_mem_block, i, &data2, sizeof(data2), &size2), OK);
        EXPECT_EQ(size1, size2);
        EXPECT_EQ(data1, data2);
    }

    EXPECT_EQ(single_mem_block->start_index, block->start_index);
    EXPECT_EQ(single_mem_block->first_offset, block->first_offset);
    EXPECT_EQ(single_mem_block->last_offset, block->last_offset);
    EXPECT_EQ(single_mem_block->ele_count, block->ele_count);
    EXPECT_EQ(single_mem_block->offset_array->size, block->offset_array->size);

    mem_block_destroy(block);
}

TEST_F(FileBlockTest, at_ok)
{
    int     data1, size1;
    int     data2, size2;

    ASSERT_EQ(file_block_write(&file_block, 0, single_mem_block), OK);

    for (int i = 0; i < single_mem_block->ele_count; i++) {
        ASSERT_EQ(file_block_at(&file_block, i, &data1, sizeof(data1), &size1), OK);
        ASSERT_EQ(mem_block_at(single_mem_block, i, &data2, sizeof(data2), &size2), OK);
        EXPECT_EQ(size1, size2);
        EXPECT_EQ(data1, data2);
    }
}


int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
