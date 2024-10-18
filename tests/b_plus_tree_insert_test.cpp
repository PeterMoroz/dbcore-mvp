#include <dbcore/b_plus_tree.h>
#include <dbcore/pages_manager.h>
#include <dbcore/coretypes.h>

#include <dbcore/column.h>
#include <dbcore/schema.h>
#include <dbcore/tuple.h>
#include <dbcore/tuple_compare.h>
#include <dbcore/rid.h>
#include <dbcore/value.h>

#include <gtest/gtest.h>
#include <array>

#include <iostream>

using namespace dbcore;

TEST(BPlusTreeTests, InsertTest1)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 2;
    const uint16_t internal_max_size = 3;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::array<uint16_t, 5> keys{ 1, 2, 3, 4, 5 };

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(bplus_tree.Insert(tuple.GetData(), rid));        
    }

    size_t num_retrieved = 0;
    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(bplus_tree.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
        num_retrieved++;
    }

    EXPECT_EQ(num_retrieved, keys.size());
}

TEST(BPlusTreeTests, InsertTest2)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 2;
    const uint16_t internal_max_size = 3;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::array<uint16_t, 5> keys{ 1, 2, 3, 4, 5 };

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(bplus_tree.Insert(tuple.GetData(), rid));
    }

    size_t num_retrieved = 0;
    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(bplus_tree.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));

        num_retrieved++;
    }

    EXPECT_EQ(num_retrieved, keys.size());

    {
        uint16_t start_key = 1;
        uint16_t current_key = start_key;

        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(start_key)} };
        Tuple tuple{values, 1, schema};

        auto itr = bplus_tree.Begin(tuple.GetData());
        for (; itr != bplus_tree.End(); ++itr) {
            const RID rid{*itr};
            EXPECT_EQ(rid.GetPageId(), current_key);
            EXPECT_EQ(rid.GetSlotId(), current_key);
            current_key += 1;
        }

        EXPECT_EQ(current_key, keys.size() + 1);
    }

    {
        uint16_t start_key = 3;
        uint16_t current_key = start_key;

        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(start_key)} };
        Tuple tuple{values, 1, schema};

        auto itr = bplus_tree.Begin(tuple.GetData());
        for (; itr != bplus_tree.End(); ++itr) {
            const RID rid{*itr};
            EXPECT_EQ(rid.GetPageId(), current_key);
            EXPECT_EQ(rid.GetSlotId(), current_key);
            current_key += 1;
        }

        EXPECT_EQ(current_key, keys.size() + 1);
    }

}

TEST(BPlusTreeTests, InsertTest3)
{
    constexpr uint32_t num_of_pages = 10;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 3;
    const uint16_t internal_max_size = 4;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::array<uint16_t, 13> keys{ 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20 };

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        // std::cout << " insert key: " << k << std::endl;
        ASSERT_TRUE(bplus_tree.Insert(tuple.GetData(), rid));
        // bplus_tree.PrintTree(std::cout);
    }

    // std::cout << " ---- insert complete ---- " << std::endl;
    // bplus_tree.PrintTree(std::cout);

    size_t num_retrieved = 0;
    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        // std::cout << " lookup key " << k << std::endl;
        ASSERT_TRUE(bplus_tree.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));

        num_retrieved++;
    }

    EXPECT_EQ(num_retrieved, keys.size());
}
