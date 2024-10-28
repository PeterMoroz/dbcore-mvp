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

void InsertHelper(BPlusTree* bplus_tree, const std::vector<uint16_t>& keys, 
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
        bplus_tree->Insert(tuple.GetData(), rid);
        // {
        //     std::lock_guard guard(m);
        //     trace << "insert:" << k << std::endl;
        // }
    }
}

void InsertHelperSplit(BPlusTree* bplus_tree, const std::vector<uint16_t>& keys, uint8_t num_threads, uint8_t thread_idx) 
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
            bplus_tree->Insert(tuple.GetData(), rid);
        }
    }
}

void DeleteHelper(BPlusTree* bplus_tree, const std::vector<uint16_t>& keys, 
                    __attribute__((unused)) uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // remove key/value by key
    for (auto k : keys) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
        Tuple tuple{values, 1, schema};
        bplus_tree->Remove(tuple.GetData());
        // {
        //     std::lock_guard guard(m);
        //     trace << "delete:" << k << std::endl;
        // }
    }
}

void DeleteHelperSplit(BPlusTree* bplus_tree, const std::vector<uint16_t>& keys, uint8_t num_threads, uint8_t thread_idx) 
{
    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    // insert a few pairs (key, value)
    for (auto k : keys) {
        if (k % num_threads == thread_idx) {
            Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
            Tuple tuple{values, 1, schema};
            bplus_tree->Remove(tuple.GetData());
        }
    }
}

void LookupHelper(BPlusTree* bplus_tree, const std::vector<uint16_t>& keys, 
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
        ASSERT_TRUE(bplus_tree->GetValue(tuple.GetData(), rid));
        EXPECT_EQ(rid, RID(k, k));
    }
}

}



TEST(BPlusTreeConcurrentTest, InsertTest1)
{
    constexpr uint32_t num_of_pages = 100;
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
    constexpr uint16_t keys_count = 100;
    for (uint16_t k = 1; k < keys_count; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelper, &bplus_tree, keys);

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

}

TEST(BPlusTreeConcurrentTest, InsertTest2)
{
    constexpr uint32_t num_of_pages = 100;
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
    constexpr uint16_t keys_count = 100;
    for (uint16_t k = 1; k < keys_count; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelperSplit, &bplus_tree, keys, 2);

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

TEST(BPlusTreeConcurrentTest, DeleteTest1)
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

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5 };
    InsertHelper(&bplus_tree, keys, 0);

    std::vector<uint16_t> remove_keys{ 1, 5, 3, 4 };
    LaunchParallelTest(2, DeleteHelper, &bplus_tree, remove_keys);


    uint16_t start_key = 2;
    uint16_t size = 0;
    uint16_t current_key = start_key;

    Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(start_key)} };
    Tuple tuple{values, 1, schema};

    auto itr = bplus_tree.Begin(tuple.GetData());
    for (; itr != bplus_tree.End(); ++itr) {
        const RID rid{*itr};
        EXPECT_EQ(rid.GetPageId(), current_key);
        EXPECT_EQ(rid.GetSlotId(), current_key);
        current_key += 1;
        size += 1;
    }

    EXPECT_EQ(size, (keys.size() - remove_keys.size()));
}

TEST(BPlusTreeConcurrentTest, DeleteTest2)
{
    constexpr uint32_t num_of_pages = 20;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 2;
    const uint16_t internal_max_size = 3;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    InsertHelper(&bplus_tree, keys, 0);

    std::vector<uint16_t> remove_keys{ 1, 4, 3, 2, 5, 6 };
    LaunchParallelTest(2, DeleteHelperSplit, &bplus_tree, remove_keys, 2);


    uint16_t start_key = 7;
    uint16_t size = 0;
    uint16_t current_key = start_key;

    Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(start_key)} };
    Tuple tuple{values, 1, schema};

    auto itr = bplus_tree.Begin(tuple.GetData());
    for (; itr != bplus_tree.End(); ++itr) {
        const RID rid{*itr};
        EXPECT_EQ(rid.GetPageId(), current_key);
        EXPECT_EQ(rid.GetSlotId(), current_key);
        current_key += 1;
        size += 1;
    }

    EXPECT_EQ(size, (keys.size() - remove_keys.size()));
}

TEST(BPlusTreeConcurrentTest, MixTest1)
{
    constexpr uint32_t num_of_pages = 20;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 2;
    const uint16_t internal_max_size = 3;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

    std::vector<uint16_t> keys{ 1, 2, 3, 4, 5 };
    InsertHelper(&bplus_tree, keys, 0);

    for (uint16_t k = 6; k <= 10; k++) {
        keys.push_back(k);
    }

    LaunchParallelTest(2, InsertHelper, &bplus_tree, keys);

    std::vector<uint16_t> remove_keys{ 1, 4, 3, 5, 6 };
    LaunchParallelTest(2, DeleteHelper, &bplus_tree, remove_keys);

    uint16_t start_key = 2;
    uint16_t size = 0;

    Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(start_key)} };
    Tuple tuple{values, 1, schema};

    auto itr = bplus_tree.Begin(tuple.GetData());
    for (; itr != bplus_tree.End(); ++itr) {
        const RID rid{*itr};
        size += 1;
    }

    EXPECT_EQ(size, (keys.size() - remove_keys.size()));
}

TEST(BPlusTreeConcurrentTest, MixTest2)
{
    constexpr uint32_t num_of_pages = 100;
    PagesManager pages_manager(num_of_pages);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};
    constexpr uint16_t key_size = 8;    // size of bigint

    TupleCompare key_cmp(schema);

    const uint16_t leaf_max_size = 3;
    const uint16_t internal_max_size = 4;
    BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);


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

    InsertHelper(&bplus_tree, persistent_keys, 0);

    auto insert_job = [&](uint8_t id) { InsertHelper(&bplus_tree, impersistent_keys, id); };
    auto delete_job = [&](uint8_t id) { DeleteHelper(&bplus_tree, impersistent_keys, id); };
    auto lookup_job = [&](uint8_t id) { LookupHelper(&bplus_tree, persistent_keys, id); };

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
    
    uint16_t size = 0;
    auto itr = bplus_tree.Begin();
    for (; itr != bplus_tree.End(); ++itr) {
        const RID rid{*itr};
        if (rid.GetSlotId() == 0) {
            break;
        }
        if (rid.GetSlotId() % sieve == 0) {
            size += 1;
        }
    }

    ASSERT_EQ(size, persistent_keys.size());
}


// TEST(BPlusTreeConcurrentTest, DbgTest1)
// {
//     constexpr uint32_t num_of_pages = 100;
//     PagesManager pages_manager(num_of_pages);

//     Column key_column{"a", TypeId::BIGINT};
//     Column cols[] = { key_column };
//     Schema schema{cols, 1};
//     constexpr uint16_t key_size = 8;    // size of bigint

//     TupleCompare key_cmp(schema);

//     const uint16_t leaf_max_size = 3;
//     const uint16_t internal_max_size = 4;
//     BPlusTree bplus_tree(pages_manager, key_cmp, key_size, leaf_max_size, internal_max_size);

//     unsigned line_count = 0;
//     std::ifstream ifs("trace-in.txt");
//     while (!ifs.eof()) {
//         std::string line;
//         std::getline(ifs, line);
//         if (line.empty())
//             break;
//         line_count++;
//         auto pos = line.find(':');
//         const std::string cmd(line.substr(0, pos));
//         const std::string key(line.substr(pos + 1));
//         const uint16_t k = std::stoi(key);

//         if (cmd == "insert") {
//             Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
//             Tuple tuple{values, 1, schema};
//             RID rid{k, k};
//             std::cout << "insert " << k << std::endl;
//             bplus_tree.Insert(tuple.GetData(), rid);
//         } else {
//             Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(k)} };
//             Tuple tuple{values, 1, schema};
//             std::cout << "remove " << k << std::endl;
//             if (k == 14) {
//                 std::cout << "stop at here " << std::endl;
//             }
//             bplus_tree.Remove(tuple.GetData());
//         }
//         bplus_tree.PrintTree(std::cout);

//         uint16_t size = 0;
//         auto itr = bplus_tree.Begin();
//         for (; itr != bplus_tree.End(); ++itr) {
//             const RID rid{*itr};
//             if (rid.GetSlotId() == 0) {
//                 break;
//             }
//             if (rid.GetSlotId() % 5 == 0) {
//                 size += 1;
//             }
//         }

//         if (line_count >= 10) {
//             ASSERT_EQ(size, 10);
//         }

//     }

// }
