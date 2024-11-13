#include <dbcore/extendible_hash_table_index.h>
#include <dbcore/extendible_hash_table.h>
#include <dbcore/tuple.h>

#include <cassert>

using namespace dbcore;

ExtendibleHashTableIndex::ExtendibleHashTableIndex(PagesManager& pages_manager, const TupleCompare& key_compare, 
                                                    const TupleHash& key_hash, uint32_t key_size)
    : _hash_table(pages_manager, key_compare, key_hash, key_size)
{

}

bool ExtendibleHashTableIndex::InsertEntry(const Tuple& key, const RID& rid)
{
    return _hash_table.Insert(key.GetData(), rid);
}

void ExtendibleHashTableIndex::DeleteEntry(const Tuple& key)
{
    _hash_table.Remove(key.GetData());
}

bool ExtendibleHashTableIndex::SearchEntry(const Tuple& key, RID* result) const
{
    assert(result);
    return _hash_table.GetValue(key.GetData(), *result);
}
