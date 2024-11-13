#pragma once

#include <dbcore/extendible_hash_table.h>

namespace dbcore
{

class Tuple;
class PagesManager;
class TupleCompare;
class TupleHash;

class ExtendibleHashTableIndex final
{
public:
    ExtendibleHashTableIndex(const ExtendibleHashTableIndex&) = delete;
    ExtendibleHashTableIndex& operator=(const ExtendibleHashTableIndex&) = delete;

    ExtendibleHashTableIndex(PagesManager& pages_manager, const TupleCompare& key_compare, 
                            const TupleHash& key_hash, uint32_t key_size);

public:
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

private:
    ExtendibleHashTable _hash_table;
};

}
