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
#include <algorithm>
#include <chrono>
#include <random>

using namespace dbcore;

TEST(BPlusTreeTests, DeleteTest1)
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


    std::array<uint16_t, 2> remove_keys{ 1, 5 };

    // remove a few keys
    for (auto k : remove_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        bplus_tree.Remove(tuple.GetData());
    }


    num_retrieved = 0;
    // check remained values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        const bool is_present = bplus_tree.GetValue(tuple.GetData(), rid);
        if (is_present) {
            EXPECT_EQ(rid, RID(k, k));
            num_retrieved++;
        } else {
            EXPECT_NE(std::find(remove_keys.cbegin(), remove_keys.cend(), k), remove_keys.cend());
        }
    }

    EXPECT_EQ(num_retrieved, keys.size() - remove_keys.size());
}

TEST(BPlusTreeTests, DeleteTest2)
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

    std::array<uint16_t, 4> remove_keys{ 1, 5, 3, 4 };

    // remove a few keys
    for (auto k : remove_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        bplus_tree.Remove(tuple.GetData());
    }

    num_retrieved = 0;
    // check remained values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        const bool is_present = bplus_tree.GetValue(tuple.GetData(), rid);
        if (is_present) {
            EXPECT_EQ(rid, RID(k, k));
            num_retrieved++;
        } else {
            EXPECT_NE(std::find(remove_keys.cbegin(), remove_keys.cend(), k), remove_keys.cend());
        }
    }

    EXPECT_EQ(num_retrieved, keys.size() - remove_keys.size());    
}

TEST(BPlusTreeTests, DeleteTest3)
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
        // std::cout << " insert key " << k << std::endl;
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
        ASSERT_TRUE(bplus_tree.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
        num_retrieved++;
    }

    EXPECT_EQ(num_retrieved, keys.size());


    std::array<uint16_t, 13> remove_keys{ keys };
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(remove_keys.begin(), remove_keys.end(), std::default_random_engine(seed));

    // for (auto k : remove_keys) {
    //     std::cout << k << ' ';
    // }
    // std::cout << std::endl;

    // std::array<uint16_t, 13> remove_keys{ 17, 9, 25, 18, 29, 33, 21, 19, 20, 37, 13, 5, 1 };

    for (auto k : remove_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        // std::cout << " remove key " << k << std::endl;        
        bplus_tree.Remove(tuple.GetData());
        // bplus_tree.PrintTree(std::cout);
    }

    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        const bool is_present = bplus_tree.GetValue(tuple.GetData(), rid);
        EXPECT_FALSE(is_present);
    }
}
