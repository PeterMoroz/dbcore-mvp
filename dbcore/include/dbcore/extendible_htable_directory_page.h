#pragma once

#include <dbcore/coretypes.h>

namespace dbcore
{

class ExtendibleHTableDirectoryPage final
{
    ExtendibleHTableDirectoryPage(const ExtendibleHTableDirectoryPage&) = delete;
    ExtendibleHTableDirectoryPage& operator=(const ExtendibleHTableDirectoryPage&) = delete;

public:

    /**
     * Initialize a new directory page after creation with pages manger.
     * @param max_depth the max depth of the directory page
    */
    void Init(uint32_t max_depth);

    /**
     * Get the bucket index that the key is hashed to.
     * @param hash the hash of the key
     * @return bucket index current key is hashed to
    */
    uint32_t HashToBucketIndex(uint32_t hash) const;

    /**
     * Lookup a bucket page using a directory index
     * @param bucket_idx the index in the directory to lookup
     * @return bucket's page_id corresponding to bucket index
    */
    page_id_t GetBucketPageId(uint32_t bucket_idx) const;

    /**
     * Updates the directory index using a bucket index and page id.
     * @param bucket_idx the index of bucket in directory
     * @param bucket_page_id page_id to set at the given index
    */
    void SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id);

    /**
     * Get the local depth of the bucket with given index
     * @param bucket_idx the index of bucket to get depth
     * @return the value of bucket's depth
    */
    uint32_t GetLocalDepth(uint32_t bucket_idx) const;

    /**
     * Set the local depth of the bucket with given index
     * @param bucket_idx the index of bucket to update
     * @param local_depth the new value of depth
    */
    void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth);

    /**
     * Increment the global depth of the directory
    */
    void IncrGlobalDepth();

    /**
     * Decrement the global depth of the directory
    */
    void DecrGlobalDepth();

    /**
     * @return true if the directory can shrunk
    */
    bool CanShrink() const;

    /**
     * @return the current directory size
    */
    uint32_t Size() const { return (1 << _global_depth); }

    /**
     * Verify the folloing invariants:
     * 1. all local_depth <= global_depth
     * 2. each bucket has precisely 2 ^ (global_depth - local_depth) pointer pointing to it
     * 3. the local_depth is the same at each index with the same bucket_page_id
    */
    bool VerifyIntegrity() const;


private:
    static constexpr uint32_t HTABLEDIRECTORY_PAGE_META_SIZE = 2 * sizeof(uint32_t);
    static constexpr uint32_t HTABLE_DIRECTORY_MAX_DEPTH = 9;  // enough at first time. TO DO: revise later, taking into consideration PAGE_SIZE
    static constexpr uint32_t HTABLE_DIRECTORY_ARRAY_SIZE = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

private:
    uint32_t _max_depth{0};
    uint32_t _global_depth{0};
    uint8_t _local_depths[HTABLE_DIRECTORY_ARRAY_SIZE];
    page_id_t _bucket_page_ids[HTABLE_DIRECTORY_ARRAY_SIZE];
};

static_assert(sizeof(page_id_t) == 4);
static_assert(sizeof(ExtendibleHTableDirectoryPage) <= PAGE_SIZE);

}
