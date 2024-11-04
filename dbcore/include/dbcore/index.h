#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/schema.h>

#include <array>


namespace dbcore
{

class Tuple;
class RID;
class PagesManager;

enum class IndexType { BPlusTreeIndex, HashTableIndex };

/**
 * IndexMetadata holds metadata of an index object.
 * 
 * The object of metadata maintains the index schema and key attribute of an index,
 * since external caller doesn't know the actual structure of the index key, so it
 * is the index's responsibility to maintain such a mapping relation and does 
 * conversion between tuple key and index key.
*/
class IndexMetadata final
{
public:
    IndexMetadata(uint32_t key_attrs[], uint32_t key_attrs_count, 
                const Schema& key_schema, const Schema& tbl_schema);

    const Schema& GetKeySchema() const { return _key_schema; }
    const Schema& GetTableSchema() const { return _tbl_schema; }

    uint32_t GetKeyAttrCount() const { return _key_attrs_count; }
    const std::array<uint32_t, MAX_COLUMN_COUNT>& GetKeyAttributes() const
    {
        return _key_attrs;
    }

private:
    /** The mapping relation between key schema and tuple schema */
    std::array<uint32_t, MAX_COLUMN_COUNT> _key_attrs;
    /** The actual number of key attributes */
    uint32_t _key_attrs_count{0};
    /** The schema of key */
    Schema _key_schema;
    /** The schema of table */
    Schema _tbl_schema;
};

class Index
{
    Index(const Index&) = delete;
    Index& operator=(const Index&) = delete;

public:
    Index(IndexType type, const IndexMetadata& metadata, PagesManager& pages_manager);

    ~Index();

    /** Insert entry into the index
     * @param tuple The indexed tuple
     * @param rid The RID associated with the \ref tuple
     * @return whether insertion is successfull
    */
    bool InsertEntry(const Tuple& tuple, const RID& rid);

    /**
     * Delete an index entry matched the given tuple
     * @param tuple The tuple to search and delete
    */
    void DeleteEntry(const Tuple& tuple);

    /**
     * Search the index for the given tuple
     * @param tuple The tuple to search
     * @param result The pointer to memory area where 
     * to write RID associated with key (if found)
     * @return whether key is found or not
    */
    bool SearchEntry(const Tuple& tuple, RID* result) const;

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

    /** The index metadata, needed to make a key by the given tuple */
    IndexMetadata _metadata;

    /** Pointer (an owning) to concrete instance of index implementation which depends 
     * on type. All request to the Index instance will be proxied to the instance of type,
     * which is kept in the field  \ref _type. Such design solution is explained by need
     * to avoid virtual functions and dynamic polymorphism.
    */
    void *_pimpl{nullptr};
};

}
