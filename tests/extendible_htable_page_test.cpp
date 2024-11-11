#include <dbcore/extendible_htable_bucket_page.h>
#include <dbcore/extendible_htable_header_page.h>
#include <dbcore/extendible_htable_directory_page.h>
#include <dbcore/pages_manager.h>
#include <dbcore/page_guard.h>
#include <dbcore/coretypes.h>

#include <dbcore/column.h>
#include <dbcore/schema.h>
#include <dbcore/tuple.h>
#include <dbcore/tuple_compare.h>
#include <dbcore/rid.h>
#include <dbcore/value.h>

#include <gtest/gtest.h>


using namespace dbcore;

TEST(ExtendibleHTableTest, BucketPageSampleTest)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    page_id_t bucket_page_id = INVALID_PAGE_ID;

    auto guard = pages_manager.NextFreePageGuarded(&bucket_page_id);
    auto bucket_page = guard.AsMut<ExtendibleHTableBucketPage>();

    constexpr uint16_t key_size = 8;
    constexpr uint16_t max_num_items = 10;

    bucket_page->Init(key_size, max_num_items);

    Column key_column{"a", TypeId::BIGINT};
    Column cols[] = { key_column };
    Schema schema{cols, 1};

    TupleCompare key_cmp(schema);

    // insert a few pairs (key, value)
    for (uint16_t i = 0; i < max_num_items; i++) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(i)} };
        Tuple tuple{values, 1, schema};
        RID rid{i, i};
        ASSERT_TRUE(bucket_page->Insert(tuple.GetData(), key_cmp, rid));
    }

    {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(max_num_items + 1)} };
        Tuple tuple{values, 1, schema};
        RID rid{max_num_items + 1, max_num_items + 1};

        // bucket is full, no more insert allowed
        ASSERT_TRUE(bucket_page->IsFull());
        ASSERT_FALSE(bucket_page->Insert(tuple.GetData(), key_cmp, rid));
    }

    // check inserted values
    for (uint16_t i = 0; i < max_num_items; i++) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(i)} };
        Tuple tuple{values, 1, schema};
        RID rid;
        ASSERT_TRUE(bucket_page->Lookup(tuple.GetData(), key_cmp, rid));
        ASSERT_EQ(rid, RID(i, i));
    }

    // remove a few pairs
    for (uint16_t i = 0; i < max_num_items; i++) {
        if (i % 2 == 1) {
            Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(i)} };
            Tuple tuple{values, 1, schema};
            ASSERT_TRUE(bucket_page->Remove(tuple.GetData(), key_cmp));
        }
    }

    // remove others, check that previous removed
    for (uint16_t i = 0; i < max_num_items; i++) {
        Value values[] = { Value{TypeId::BIGINT, static_cast<int64_t>(i)} };
        Tuple tuple{values, 1, schema};
        if (i % 2 == 1) {
            ASSERT_FALSE(bucket_page->Remove(tuple.GetData(), key_cmp));
        } else {
            ASSERT_TRUE(bucket_page->Remove(tuple.GetData(), key_cmp));
        }
    }

    ASSERT_TRUE(bucket_page->IsEmpty());
}

TEST(ExtendibleHTableTest, HeaderDirectoryPageSampleTest)
{
    constexpr uint32_t num_of_pages = 6;
    PagesManager pages_manager(num_of_pages);

    page_id_t header_page_id = INVALID_PAGE_ID;
    page_id_t directory_page_id = INVALID_PAGE_ID;

    page_id_t bucket_page_id_1 = INVALID_PAGE_ID;
    page_id_t bucket_page_id_2 = INVALID_PAGE_ID;
    page_id_t bucket_page_id_3 = INVALID_PAGE_ID;
    page_id_t bucket_page_id_4 = INVALID_PAGE_ID;

    {
        // HeaderPage test
        auto guard = pages_manager.NextFreePageGuarded(&header_page_id);
        auto header_page = guard.AsMut<ExtendibleHTableHeaderPage>();
        header_page->Init(2);

        /* Test Hashes for header page
          00000000000000001000000000000000 - 32768
          01000000000000001000000000000000 - 1073774592
          10000000000000001000000000000000 - 2147516416
          11000000000000001000000000000000 - 3221258240
        */
        
        // ensure that we are hashing into proper bucket based on upper 2 bits
        uint32_t hashes[4] = { 32768, 1073774592, 2147516416, 3221258240 };
        for (uint32_t i = 0; i < 4; i++) {
            ASSERT_EQ(header_page->HashToDirectoryIndex(hashes[i]), i);
        }
    }


    {
        // DirectoryPage test
        auto directory_guard = pages_manager.NextFreePageGuarded(&directory_page_id);
        auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
        directory_page->Init(3);

        auto bucket_page_guard_1 = pages_manager.NextFreePageGuarded(&bucket_page_id_1);
        auto bucket_page_1 = bucket_page_guard_1.AsMut<ExtendibleHTableBucketPage>();
        bucket_page_1->Init(10);

        auto bucket_page_guard_2 = pages_manager.NextFreePageGuarded(&bucket_page_id_2);
        auto bucket_page_2 = bucket_page_guard_1.AsMut<ExtendibleHTableBucketPage>();
        bucket_page_2->Init(10);

        auto bucket_page_guard_3 = pages_manager.NextFreePageGuarded(&bucket_page_id_3);
        auto bucket_page_3 = bucket_page_guard_1.AsMut<ExtendibleHTableBucketPage>();
        bucket_page_3->Init(10);

        auto bucket_page_guard_4 = pages_manager.NextFreePageGuarded(&bucket_page_id_4);
        auto bucket_page_4 = bucket_page_guard_1.AsMut<ExtendibleHTableBucketPage>();
        bucket_page_4->Init(10);

        directory_page->SetBucketPageId(0, bucket_page_id_1);

        /*
         ===== Directory (global depth: 0) ====
         | bucket idx | page id | local depth |
         |  0         |  2      |  0          |
         ============ end directory ===========
        */

        ASSERT_TRUE(directory_page->VerifyIntegrity());
        ASSERT_EQ(directory_page->Size(), 1);
        ASSERT_EQ(directory_page->GetBucketPageId(0), bucket_page_id_1);


        // grow the directory, local depths should change
        directory_page->SetLocalDepth(0, 1);
        directory_page->IncrGlobalDepth();
        directory_page->SetBucketPageId(1, bucket_page_id_2);
        directory_page->SetLocalDepth(1, 1);


        /*
         ===== Directory (global depth: 1) ====
         | bucket idx | page id | local depth |
         |  0         |  2      |  1          |
         |  1         |  3      |  1          |
         ============ end directory ===========
        */

        ASSERT_TRUE(directory_page->VerifyIntegrity());
        ASSERT_EQ(directory_page->Size(), 2);
        ASSERT_EQ(directory_page->GetBucketPageId(0), bucket_page_id_1);
        ASSERT_EQ(directory_page->GetBucketPageId(1), bucket_page_id_2);

        for (uint32_t i = 0; i < 100; i++) {
            ASSERT_EQ(directory_page->HashToBucketIndex(i), i % 2);
        }


        directory_page->SetLocalDepth(0, 2);
        directory_page->IncrGlobalDepth();
        directory_page->SetBucketPageId(2, bucket_page_id_3);

        /*
         ===== Directory (global depth: 2) ====
         | bucket idx | page id | local depth |
         |  0         |  2      |  2          |
         |  1         |  3      |  1          |
         |  2         |  4      |  2          |
         |  3         |  3      |  1          |
         ============ end directory ===========
        */

        ASSERT_TRUE(directory_page->VerifyIntegrity());
        ASSERT_EQ(directory_page->Size(), 4);
        ASSERT_EQ(directory_page->GetBucketPageId(0), bucket_page_id_1);
        ASSERT_EQ(directory_page->GetBucketPageId(1), bucket_page_id_2);
        ASSERT_EQ(directory_page->GetBucketPageId(2), bucket_page_id_3);
        ASSERT_EQ(directory_page->GetBucketPageId(3), bucket_page_id_2);

        for (uint32_t i = 0; i < 100; i++) {
            ASSERT_EQ(directory_page->HashToBucketIndex(i), i % 4);
        }


        directory_page->SetLocalDepth(0, 3);
        directory_page->IncrGlobalDepth();
        directory_page->SetBucketPageId(4, bucket_page_id_4);

        /*
         ===== Directory (global depth: 3) ====
         | bucket idx | page id | local depth |
         |  0         |  2      |  3          |
         |  1         |  3      |  1          |
         |  2         |  4      |  2          |
         |  3         |  3      |  1          |
         |  4         |  5      |  3          |
         |  5         |  3      |  1          |
         |  6         |  4      |  2          |
         |  7         |  3      |  1          |
         ============ end directory ===========
        */

        ASSERT_TRUE(directory_page->VerifyIntegrity());
        ASSERT_EQ(directory_page->Size(), 8);
        ASSERT_EQ(directory_page->GetBucketPageId(0), bucket_page_id_1);
        ASSERT_EQ(directory_page->GetBucketPageId(1), bucket_page_id_2);
        ASSERT_EQ(directory_page->GetBucketPageId(2), bucket_page_id_3);
        ASSERT_EQ(directory_page->GetBucketPageId(3), bucket_page_id_2);
        ASSERT_EQ(directory_page->GetBucketPageId(4), bucket_page_id_4);
        ASSERT_EQ(directory_page->GetBucketPageId(5), bucket_page_id_2);
        ASSERT_EQ(directory_page->GetBucketPageId(6), bucket_page_id_3);
        ASSERT_EQ(directory_page->GetBucketPageId(7), bucket_page_id_2);

        for (uint32_t i = 0; i < 100; i++) {
            ASSERT_EQ(directory_page->HashToBucketIndex(i), i % 8);
        }


        // at this time we can't shrink the directory since local_depth = global_depth = 3
        ASSERT_EQ(directory_page->CanShrink(), false);

        directory_page->SetLocalDepth(0, 2);
        directory_page->SetLocalDepth(4, 2);
        directory_page->SetBucketPageId(0, bucket_page_id_4);


        /*
         ===== Directory (global depth: 3) ====
         | bucket idx | page id | local depth |
         |  0         |  5      |  2          |
         |  1         |  3      |  1          |
         |  2         |  4      |  2          |
         |  3         |  3      |  1          |
         |  4         |  5      |  2          |
         |  5         |  3      |  1          |
         |  6         |  4      |  2          |
         |  7         |  3      |  1          |
         ============ end directory ===========
        */

        ASSERT_EQ(directory_page->CanShrink(), true);
        directory_page->DecrGlobalDepth();


        /*
         ===== Directory (global depth: 2) ====
         | bucket idx | page id | local depth |
         |  0         |  5      |  2          |
         |  1         |  3      |  1          |
         |  2         |  4      |  2          |
         |  3         |  3      |  1          |
         ============ end directory ===========
        */

        ASSERT_TRUE(directory_page->VerifyIntegrity());
        ASSERT_EQ(directory_page->Size(), 4);
        ASSERT_EQ(directory_page->CanShrink(), false);
    }
}
