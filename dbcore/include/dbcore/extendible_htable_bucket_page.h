#pragma once

#include <dbcore/coretypes.h>

namespace dbcore
{

class RID;
class TupleCompare;

class ExtendibleHTableBucketPage final
{
    ExtendibleHTableBucketPage(const ExtendibleHTableBucketPage&) = delete;
    ExtendibleHTableBucketPage& operator=(const ExtendibleHTableBucketPage&) = delete;

public:

    void Init(uint32_t key_size);
    void Init(uint32_t key_size, uint32_t max_num_items);


    /**
     * Attempts to insert a key and value in the bucket.
     * @param key the key to insert
     * @param key_size the length of key on bytes
     * @param cmp the key comparator to use
     * @param rid the value
     * @return true if inserted, false when bucket is full or the same key is already present
    */
    bool Insert(const char* key, uint32_t key_size, const TupleCompare& cmp, const RID& rid);

    /**
     * Removes a key and value
     * @param key the key to remove
     * @param key_size the length of key on bytes
     * @param cmp the key comparator to use
     * @return true if removed, false if not found
    */
    bool Remove(const char* key, uint32_t key_size, const TupleCompare& cmp);

    /**
     * Lookup a value with a given key
     * @param key the key to lookup
     * @param key_size the length of the key in bytes
     * @param cmp the key comparator to use
     * @param rid the value if found will be written here
     * @return true if the key and value are present, false if not found
    */
    bool Lookup(const char* key, uint32_t key_size, const TupleCompare& cmp, RID& rid) const;

    /** @return whether bucket is full */
    bool IsFull() const { return _num_items == _max_num_items; }

    /** return whether bucket is empty */
    bool IsEmpty() const { return _num_items == 0; }

    void Print() const;

private:
    bool bsearch(const char* key, uint32_t key_size, const TupleCompare& cmp, uint32_t* pos) const;

private:
    static uint32_t MaxNumItems(uint32_t key_size);

    static constexpr uint32_t HTABLE_BUCKET_PAGE_META_SIZE = 3 * sizeof(uint32_t);
    static constexpr uint32_t HTABLE_BUCKET_PAGE_DATA_SIZE = (PAGE_SIZE - HTABLE_BUCKET_PAGE_META_SIZE);

private:
    /** The lengh of the key in bytes */
    uint32_t _key_size{0};
    /** The number of stored items */
    uint32_t _num_items{0};
    /** The maximal allowed number of items */
    uint32_t _max_num_items{0};

    char _data[];
};

}
