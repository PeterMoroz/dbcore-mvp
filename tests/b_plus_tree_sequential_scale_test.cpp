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

#include <algorithm>
#include <random>
#include <vector>

#include <iostream>

using namespace dbcore;

TEST(BPlusTreeTests, ScaleTest)
{
    constexpr uint32_t num_of_pages = 700;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 3;
    const uint16_t internal_max_size = 4;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::vector<uint16_t> keys;
    constexpr uint16_t scale = 1000;
    for (uint16_t i = 1; i < scale; i++)
        keys.push_back(i);

    auto rng = std::default_random_engine{};
    std::shuffle(keys.begin(), keys.end(), rng);

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        // std::cout << " insert key " << k << std::endl;
        ASSERT_TRUE(bplus_tree.Insert(tuple.GetData(), rid));
        // bplus_tree.PrintTree(std::cout);
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
