#pragma once

#include <dbcore/coretypes.h>

#include <shared_mutex>

namespace dbcore
{

class PagesManager;
class TupleCompare;
class TupleHash;
class RID;

class ExtendibleHTableHeaderPage;
class ExtendibleHTableDirectoryPage;
class ExtendibleHTableBucketPage;

/**
 * Implementation of extendible hash table whichis backed by pages manager.
 * Non-unique keys are not supported. Support insert and delete. The table 
 * grows/shrinks dynamically as buckets become full/empty.
*/
class ExtendibleHashTable final
{
    ExtendibleHashTable(const ExtendibleHashTable&) = delete;
    ExtendibleHashTable& operator=(const ExtendibleHashTable&) = delete;

public:

    /**
     * Construct an instance of ExtentibleHashTable
     * @param pages_manager pages manager to be used
     * @param tuple_compare comparator (compare functor) for keys
     * @param tuple_hash the hashing functor for keys
     * @param header_max_depth the maximal depth allowed for the header page (0 means that page will be initialized with its default value)
     * @param directory_max_depth the maximal depth allowed for the directory page (0 means that page will be initialized with its default value)
     * @param bucket_max_size the maximal size allowed for the bucket page array (0 means that page will be initialized with its default value)
    */
    ExtendibleHashTable(PagesManager& pages_manager, const TupleCompare& tuple_compare, 
                        const TupleHash& tuple_hash, const uint32_t key_size, 
                        uint32_t header_max_depth = 0, uint32_t directory_max_depth = 0, uint32_t bucket_max_size = 0);

    ~ExtendibleHashTable();

    /**
     * Inserts a key-value pair into the hash table
     * @param key the key to create
     * @param rid the value to be associated with the key
     * @return true id insert succeeded, false otherwise
    */
    bool Insert(const char* key, const RID& rid);

    /**
     * Removes a key-value pair from the hash table
     * @param key the key to remove
     * @return true if remove succeeded, false otherwise
    */
    bool Remove(const char *key);

    /**
     * Get the value associated with a given key
     * @param key the key to look up
     * @param[out] rid the value associated with a given key
     * @return true if the key was found and value assigned, false otherwise
    */
    bool GetValue(const char* key, RID& rid) const;
    
    /**
     * Helper function to verify integrity of the extendible hash table directory
     * @return true when all directory pages keep invariants, false otherwse
    */
    bool VerifyIntegrity() const;

private:
    bool InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                            uint32_t hash, const char* key, const RID& rid);
    
    bool InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                            const char* key, const RID& rid);

    void UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx,
                                page_id_t new_bucket_page_id, uint32_t new_local_depth);

    bool InsertToBucket(ExtendibleHTableBucketPage *bucket, ExtendibleHTableDirectoryPage *directory,
                        uint32_t bucket_idx, const char* key, const RID& rid);


private:
    PagesManager& _pages_manager;
    const TupleCompare& _key_compare;
    const TupleHash& _key_hash;
    const uint32_t _key_size{0};
    uint32_t _header_max_depth{0};
    uint32_t _directory_max_depth{0};
    uint32_t _bucket_max_size{0};
    page_id_t _header_page_id{INVALID_PAGE_ID};
    mutable std::shared_mutex _mutex;
};

}
