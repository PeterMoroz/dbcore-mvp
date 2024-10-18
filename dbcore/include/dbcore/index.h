#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/schema.h>

#include <array>


namespace dbcore
{

class Tuple;
class RID;

enum class IndexType { BPlusTreeIndex, HashTableIndex };

/**
 * IndexMetadata holds metadat of an index object.
 * 
 * The object of metadata maintains the index schema and key attribute of an index,
 * since external caller doesn't know the actual structure of the index key, so it
 * is the index's responsibility to maintain such a mapping relation and does 
 * conversion between tuple key and index key.
*/
class IndexMetadata final
{
public:
    IndexMetadata(uint32_t key_attrs[], uint32_t key_attrs_count, const Schema& schema);

private:
    /** The mapping relation between key schema and tuple schema */
    std::array<uint32_t, MAX_COLUMN_COUNT> _key_attrs;
    /** The actual number of key attributes */
    uint32_t _key_attrs_count{0};
    /** The schema of indexed key */
    Schema _schema;
};

class Index
{
    Index(const Index&) = delete;
    Index& operator=(const Index&) = delete;

public:
    Index(IndexType type, const IndexMetadata& metadata);

    ~Index();

    /** Insert entry into the index 
     * @param key The index key
     * @param rid The RID associated with the key
     * @return whether insertion is successfull
    */
    bool InsertEntry(const Tuple& key, const RID& rid);

    /**
     * Delete an index entry by key
     * @param key The index key
    */
    void DeleteEntry(const Tuple& key);

    /**
     * Search the index for the provided key
     * @param key The index key
     * @param result The pointer to memory area where 
     * to write RID associated with key (if found)
     * @return whether key is found or not
    */
    bool SearchEntry(const Tuple& key, RID* result) const;

    /* TO DO: 
      implement ScanKey method which will populate the provided 
      array of RID, if more than one is matching a given key.
      instead of ScanKey (or in additional) implement methods
      to iterate from the begining/from the given RID to the end 
      of index, will require design and implementation IndexIterator.
    */

public:
    /** The type of the index */
    IndexType _type;

    /** The Index object owns its metadata */
    IndexMetadata _metadata;

    /** Pointer (an owning) to concrete instance of index implementation which depends 
     * on type. All request to the Index instance will be proxied to the instance of type,
     * which is kept in the field  \ref _type. Such design solution is explained by need
     * to avoid virtual functions and dynamic polymorphism.
    */
    void *_pimpl{nullptr};
};

}
