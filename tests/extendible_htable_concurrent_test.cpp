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
#include <algorithm>
#include <chrono>
#include <random>
#include <thread>


// #include <fstream>
// #include <iostream>
// #include <mutex>

using namespace dbcore;

namespace
{

// std::mutex m;
// std::ofstream trace("trace-out.txt");

template <typename... Args>
void LaunchParallelTest(uint8_t num_threads, Args &&...args) 
{
    std::vector<std::thread> threads;

    for (uint8_t i = 0; i < num_threads; i++) {
        threads.push_back(std::thread(args..., i));
    }

    for (uint8_t i = 0; i < num_threads; i++) {
        threads[i].join();
    }    
}

void InsertHelper(ExtendibleHashTable* hash_table, const std::vector<uint16_t>& keys, 
                    __attribute__((unused)) uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        hash_table->Insert(tuple.GetData(), rid);
        // {
        //     std::lock_guard guard(m);
        //     trace << "insert:" << k << std::endl;
        // }
    }
}

void InsertHelperSplit(ExtendibleHashTable* hash_table, const std::vector<uint16_t>& keys, uint8_t num_threads, uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // insert a few pairs (key, value)
    for (auto k : keys) {
        if (k % num_threads == thread_idx) {
            Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
            Tuple tuple{values, 1, schema};
            RID rid{k, k};
            hash_table->Insert(tuple.GetData(), rid);
        }
    }
}

void DeleteHelper(ExtendibleHashTable* hash_table, const std::vector<uint16_t>& keys, __attribute__((unused)) uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // remove key/value by key
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        hash_table->Remove(tuple.GetData());
        // {
        //     std::lock_guard guard(m);
        //     trace << "delete:" << k << std::endl;
        // }
    }
}

void DeleteHelperSplit(ExtendibleHashTable* hash_table, const std::vector<uint16_t>& keys, uint8_t num_threads, uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // insert a few pairs (key, value)
    for (auto k : keys) {
        if (k % num_threads == thread_idx) {
            Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
            Tuple tuple{values, 1, schema};
            hash_table->Remove(tuple.GetData());
        }
    }
}

void LookupHelper(ExtendibleHashTable* hash_table, const std::vector<uint16_t>& keys, __attribute__((unused)) uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // insert a few pairs (key, value)
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid{k, k};
        ASSERT_TRUE(hash_table->GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }
}

}



TEST(ExtendibleHTableConcurrentTest, InsertTest1)
{
    constexpr uint32_t num_of_pages = 100;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> keys;
    constexpr uint16_t keys_count = 100;
    for (uint16_t k = 1; k < keys_count; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelper, &hash_table, keys);

    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }
}

TEST(ExtendibleHTableConcurrentTest, InsertTest2)
{
    constexpr uint32_t num_of_pages = 100;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> keys;
    constexpr uint16_t keys_count = 100;
    for (uint16_t k = 1; k < keys_count; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelperSplit, &hash_table, keys, 2);

    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }
}

TEST(ExtendibleHTableConcurrentTest, DeleteTest1)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5 };
    InsertHelper(&hash_table, keys, 0);

    std::vector<uint16_t> remove_keys{ 1, 5, 3, 4 };
    LaunchParallelTest(2, DeleteHelper, &hash_table, remove_keys);

    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        if (k == 2) {
            ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
            EXPECT_EQ(rid, RID(k, k));
        } else {
            ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
        }
    }
}

TEST(ExtendibleHTableConcurrentTest, DeleteTest2)
{
    constexpr uint32_t num_of_pages = 20;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    InsertHelper(&hash_table, keys, 0);

    std::vector<uint16_t> remove_keys{ 1, 4, 3, 2, 5, 6 };
    LaunchParallelTest(2, DeleteHelperSplit, &hash_table, remove_keys, 2);

    // check inserted values
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        if (k > 6) {
            ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
            EXPECT_EQ(rid, RID(k, k));
        } else {
            ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
        }
    }
}

TEST(ExtendibleHTableConcurrentTest, MixTest1)
{
    constexpr uint32_t num_of_pages = 20;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5 };
    InsertHelper(&hash_table, keys, 0);

    for (uint16_t k = 6; k <= 10; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelper, &hash_table, keys);
    std::vector<uint16_t> remove_keys{ 1, 4, 3, 5, 6 };
    LaunchParallelTest(2, DeleteHelper, &hash_table, remove_keys);

    const std::vector<uint16_t> valid_keys{ 2, 7, 8, 9, 10 };
    const std::vector<uint16_t> invalid_keys{ 1, 3, 4, 5, 6 };

    for (const auto k : valid_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }

    for (const auto k : invalid_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_FALSE(hash_table.GetValue(tuple.GetData(), rid));
    }
}

TEST(ExtendibleHTableConcurrentTest, MixTest2)
{
    constexpr uint32_t num_of_pages = 100;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);
    TupleHash key_hash(schema, dummy_hash);
    ExtendibleHashTable hash_table(pages_manager, key_cmp, key_hash, key_size);

    std::vector<uint16_t> persistent_keys;
    std::vector<uint16_t> impersistent_keys;
    const uint16_t total_keys = 50;
    const uint16_t sieve = 5;
    for (uint16_t k = 1; k <= total_keys; k++) {
        if (k % sieve == 0)
            persistent_keys.push_back(k);
        else
            impersistent_keys.push_back(k);
    }

    InsertHelper(&hash_table, persistent_keys, 1);

    auto insert_job = [&](uint8_t id) { InsertHelper(&hash_table, impersistent_keys, id); };
    auto delete_job = [&](uint8_t id) { DeleteHelper(&hash_table, impersistent_keys, id); };
    auto lookup_job = [&](uint8_t id) { LookupHelper(&hash_table, persistent_keys, id); };

    std::vector<std::thread> threads;
    std::vector<std::function<void (int)>> jobs;
    jobs.emplace_back(insert_job);
    jobs.emplace_back(delete_job);
    jobs.emplace_back(lookup_job);


    const uint8_t num_threads = 6;
    for (uint8_t i = 0; i <  num_threads; i++) {
        threads.emplace_back(std::thread{jobs[i % jobs.size()], i});
    }

    for (uint8_t i = 0; i < num_threads; i++) {
        threads[i].join();
    }
    
    for (const auto k : persistent_keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(hash_table.GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }
}
