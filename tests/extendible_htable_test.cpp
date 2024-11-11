#include <dbcore/extendible_hash_table.h>
#include <dbcore/pages_manager.h>
#include <dbcore/coretypes.h>

#include <dbcore/column.h>
#include <dbcore/schema.h>
#include <dbcore/tuple.h>
#include <dbcore/tuple_compare.h>
#include <dbcore/tuple_hash.h>
#include <dbcore/hash.h>
#include <dbcore/rid.h>
#include <dbcore/value.h>

#include <gtest/gtest.h>

#include <vector>


using namespace dbcore;

/**
 * ATTENTION !
 * These test cases are valid only when using dummy_hash function 
 * (function cast the first 4 bytes of data to unsigned 32-bit integer 
 * and return that value as result of hashing, see implementation).
*/

TEST(ExtendibleHTableTest, InsertTest1)
{
    constexpr uint32_t num_of_pages = 10;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);

    constexpr uint16_t header_max_depth = 0;
    constexpr uint16_t directory_max_depth = 2;
    constexpr uint16_t bucket_max_size = 2;
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size,
                                header_max_depth, directory_max_depth, bucket_max_size);

    constexpr uint16_t num_keys = 8;
    std::vector<uint16_t> keys;

    for (uint16_t i = 0; i < num_keys; i++) {
        keys.push_back(i);
    }

    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(hash_table.Insert(tuple.GetData(), rid));
    }

    ASSERT_TRUE(hash_table.VerifyIntegrity());

    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid_expected{k, k};
        RID rid_actual;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid_actual));
        ASSERT_EQ(rid_actual, rid_expected);
    }

    // attempt another insert should fail, because the table is full
    Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(num_keys)} };
    Tuple tuple{values, 1, schema};
    RID rid{num_keys, num_keys};
    ASSERT_FALSE(hash_table.Insert(tuple.GetData(), rid));
}

TEST(ExtendibleHTableTest, InsertTest2)
{
    constexpr uint32_t num_of_pages = 10;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);

    constexpr uint16_t header_max_depth = 2;
    constexpr uint16_t directory_max_depth = 3;
    constexpr uint16_t bucket_max_size = 2;
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size,
                                header_max_depth, directory_max_depth, bucket_max_size);

    constexpr uint16_t num_keys = 5;
    std::vector<uint16_t> keys;

    for (uint16_t i = 0; i < num_keys; i++) {
        keys.push_back(i);
    }

    // insert some keys
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(hash_table.Insert(tuple.GetData(), rid));
    }

    ASSERT_TRUE(hash_table.VerifyIntegrity());

    // check that keys were actually inserted
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid_expected{k, k};
        RID rid_actual;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid_actual));
        ASSERT_EQ(rid_actual, rid_expected);
    }

    // try to get some keys that don't exist (were not inserted)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k + num_keys)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
    }

}

TEST(ExtendibleHTableTest, RemoveTest1)
{
    constexpr uint32_t num_of_pages = 10;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);

    constexpr uint16_t header_max_depth = 2;
    constexpr uint16_t directory_max_depth = 3;
    constexpr uint16_t bucket_max_size = 2;
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size,
                                header_max_depth, directory_max_depth, bucket_max_size);

    constexpr uint16_t num_keys = 5;
    std::vector<uint16_t> keys;

    for (uint16_t i = 0; i < num_keys; i++) {
        keys.push_back(i);
    }

    // insert some keys
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(hash_table.Insert(tuple.GetData(), rid));
    }

    ASSERT_TRUE(hash_table.VerifyIntegrity());

    // check that keys were actually inserted
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid_expected{k, k};
        RID rid_actual;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid_actual));
        ASSERT_EQ(rid_actual, rid_expected);
    }

    // try to get some keys that don't exist (were not inserted)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k + num_keys)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
    }


    // remove the keys inserted before
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        ASSERT_TRUE(hash_table.Remove(tuple.GetData()));
    }

    ASSERT_TRUE(hash_table.VerifyIntegrity());

    // check that there are no more keys
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_FALSE(hash_table.Remove(tuple.GetData()));
        ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
    }

    ASSERT_TRUE(hash_table.VerifyIntegrity());
}
