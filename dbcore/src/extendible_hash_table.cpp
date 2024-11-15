#include <dbcore/extendible_hash_table.h>
#include <dbcore/pages_manager.h>
#include <dbcore/extendible_htable_header_page.h>
#include <dbcore/extendible_htable_directory_page.h>
#include <dbcore/extendible_htable_bucket_page.h>

#include <dbcore/tuple_compare.h>
#include <dbcore/tuple_hash.h>
#include <dbcore/rid.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <limits>

using namespace dbcore;


ExtendibleHashTable::ExtendibleHashTable(
        PagesManager& pages_manager, const TupleCompare& tuple_compare,
        const TupleHash& tuple_hash, const uint32_t key_size, 
        uint32_t header_max_depth /* = 0*/, uint32_t directory_max_depth /* = 0*/, uint32_t bucket_max_size /* = 0*/)
    : _pages_manager(pages_manager)
    , _key_compare(tuple_compare)
    , _key_hash(tuple_hash)
    , _key_size(key_size)
    , _header_max_depth(header_max_depth)
    , _directory_max_depth(directory_max_depth)
    , _bucket_max_size(bucket_max_size)
{
    page_id_t header_page_id{INVALID_PAGE_ID};
    auto guard = _pages_manager.NextFreePageGuarded(&header_page_id);
    assert(header_page_id != INVALID_PAGE_ID);
    
    auto header_page = guard.AsMut<ExtendibleHTableHeaderPage>();
    header_page->Init(_header_max_depth);
    _header_page_id = header_page_id;
}

ExtendibleHashTable::~ExtendibleHashTable()
{
    if (_header_page_id != INVALID_PAGE_ID) {
        // TO DO: set the dirty flag if changes are made
        _pages_manager.UnpinPage(_header_page_id, false);
    }
}


bool ExtendibleHashTable::Insert(const char* key, const RID& rid)
{
    // lock the whole tree now.
    // TO DO: think about locking with page granularity
    std::unique_lock lock(_mutex);

    auto header_guard = _pages_manager.GetPageWrite(_header_page_id);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    const uint32_t hash = _key_hash(key);
    const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    if (InsertToNewDirectory(header_page, directory_idx, hash, key, rid)) {
        return true;
    }
    return false;
}

bool ExtendibleHashTable::Remove(const char *key)
{
    // lock the whole tree now.
    std::unique_lock lock(_mutex);

    ReadPageGuard header_guard = _pages_manager.GetPageRead(_header_page_id);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    const uint32_t hash = _key_hash(key);
    const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    const page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
    if (directory_page_id == INVALID_PAGE_ID) {
        return false;
    }

    WritePageGuard directory_guard = _pages_manager.GetPageWrite(directory_page_id);
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    const page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == INVALID_PAGE_ID) {
        return false;
    }

    WritePageGuard bucket_guard = _pages_manager.GetPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage>();
    if (!bucket_page->Remove(key, _key_compare)) {
        return false;
    }

    if (bucket_page->IsEmpty()) {
        if (directory_page->GetGlobalDepth() > 0) {
            const uint32_t split_idx = directory_page->GetSplitImageIndex(bucket_idx);
            directory_page->DecrLocalDepth(split_idx);
            const page_id_t split_page_id = directory_page->GetBucketPageId(split_idx);
            const uint32_t local_depth = directory_page->GetLocalDepth(split_idx);
            UpdateDirectoryMapping(directory_page, bucket_idx, split_page_id, local_depth);
            if (directory_page->CanShrink()) {
                directory_page->DecrGlobalDepth();
            }
        }
    }

    return true;
}

bool ExtendibleHashTable::GetValue(const char* key, RID& rid) const
{
    // lock the whole tree now.
    // TO DO: think about locking with page granularity
    std::shared_lock lock(_mutex);

    auto header_guard = _pages_manager.GetPageRead(_header_page_id);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

    const uint32_t hash = _key_hash(key);
    const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    const page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
    if (directory_page_id == INVALID_PAGE_ID) {
        return false;
    }

    auto directory_guard = _pages_manager.GetPageRead(directory_page_id);
    auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
    const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    const page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == INVALID_PAGE_ID) {
        return false;
    }

    auto bucket_guard = _pages_manager.GetPageRead(bucket_page_id);
    auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage>();

    return bucket_page->Lookup(key, _key_compare, rid);
}

bool ExtendibleHashTable::VerifyIntegrity() const
{
    assert(_header_page_id != INVALID_PAGE_ID);
    auto header_guard = _pages_manager.GetPageGuarded(_header_page_id);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

    const uint32_t max_size = header_page->MaxSize();
    for (uint32_t i = 0; i < max_size; i++) {
        const page_id_t page_id = header_page->GetDirectoryPageId(i);
        if (page_id != INVALID_PAGE_ID) {
            auto directory_guard = _pages_manager.GetPageGuarded(page_id);
            auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
            if (!directory_page->VerifyIntegrity()) {
                return false;
            }
        }
    }
    return true;
}


bool ExtendibleHashTable::InsertToNewDirectory(
        ExtendibleHTableHeaderPage *header, uint32_t directory_idx, uint32_t hash, const char* key, const RID& rid)
{
    page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
    WritePageGuard directory_guard;
    if (directory_page_id == INVALID_PAGE_ID) {
        PageGuard guard = _pages_manager.NextFreePageGuarded(&directory_page_id);
        directory_guard = guard.UpgradeWrite();
        auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
        directory_page->Init(_directory_max_depth);
        header->SetDirectoryPageId(directory_idx, directory_page_id);
    } else {
        directory_guard = _pages_manager.GetPageWrite(directory_page_id);
    }

    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);

    return InsertToNewBucket(directory_page, bucket_idx, key, rid);
}

bool ExtendibleHashTable::InsertToNewBucket(
        ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx, const char* key, const RID& rid)
{
    page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
    WritePageGuard bucket_guard;
    if (bucket_page_id == INVALID_PAGE_ID) {
        PageGuard guard = _pages_manager.NextFreePageGuarded(&bucket_page_id);
        bucket_guard = guard.UpgradeWrite();
        auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage>();
        if (_bucket_max_size == 0)
            bucket_page->Init(_key_size);
        else
            bucket_page->Init(_key_size, _bucket_max_size);
        UpdateDirectoryMapping(directory, bucket_idx, bucket_page_id, 0);
    } else {
        bucket_guard = _pages_manager.GetPageWrite(bucket_page_id);
    }

    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage>();
    return InsertToBucket(bucket_page, directory, bucket_idx, key, rid);
}

void ExtendibleHashTable::UpdateDirectoryMapping(
        ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx, 
        page_id_t new_bucket_page_id, uint32_t new_local_depth)
{
    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

bool ExtendibleHashTable::InsertToBucket(ExtendibleHTableBucketPage *bucket, ExtendibleHTableDirectoryPage *directory,
                                        uint32_t bucket_idx, const char* key, const RID& rid)
{
    if (bucket->Insert(key, _key_compare, rid)) {
        return true;
    }

    // insert failed because bucket is full
    if (bucket->IsFull()) {
        if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
            if (directory->GetGlobalDepth() == directory->GetMaxDepth()) {
                return false;
            }
            directory->IncrGlobalDepth();
        }

        page_id_t split_page_id = INVALID_PAGE_ID;
        PageGuard guard = _pages_manager.NextFreePageGuarded(&split_page_id);
        WritePageGuard split_page_guard  = guard.UpgradeWrite();
        auto split_page = split_page_guard.AsMut<ExtendibleHTableBucketPage>();

        if (_bucket_max_size == 0)
            split_page->Init(_key_size);
        else
            split_page->Init(_key_size, _bucket_max_size);

        directory->IncrLocalDepth(bucket_idx);
        const uint32_t split_page_idx = directory->GetSplitImageIndex(bucket_idx);
        UpdateDirectoryMapping(directory, split_page_idx, split_page_id, directory->GetLocalDepth(bucket_idx));

        uint32_t i = 0;
        uint32_t bucket_size = bucket->NumItems();
        while (i < bucket_size) {
            const char* k = bucket->KeyAt(i);
            const RID v = bucket->ValueAt(i);
            if (directory->HashToBucketIndex(_key_hash(k)) == split_page_idx) {
                split_page->Insert(k, _key_compare, v);
                bucket->RemoveAt(i);
                bucket_size--;
                continue;
            }
            i++;
        }

        const uint32_t hash = _key_hash(key);
        const uint32_t idx = directory->HashToBucketIndex(hash);
        if (idx == bucket_idx) {
            return InsertToBucket(bucket, directory, bucket_idx, key, rid);
        } else {
            return InsertToBucket(split_page, directory, split_page_idx, key, rid);
        }

    }

    return false;
}
