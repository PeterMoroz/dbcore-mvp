#include <dbcore/b_plus_tree_index.h>
#include <dbcore/b_plus_tree.h>
#include <dbcore/tuple.h>

#include <cassert>

using namespace dbcore;

BPlusTreeIndex::BPlusTreeIndex(PagesManager& pages_manager, const TupleCompare& key_compare, uint32_t key_size)
    : _bplus_tree(pages_manager, key_compare, key_size)
{

}

bool BPlusTreeIndex::InsertEntry(const Tuple& key, const RID& rid)
{
    return _bplus_tree.Insert(key.GetData(), rid);
}

void BPlusTreeIndex::DeleteEntry(const Tuple& key)
{
    _bplus_tree.Remove(key.GetData());
}

bool BPlusTreeIndex::SearchEntry(const Tuple& key, RID* result) const
{
    assert(result);
    return _bplus_tree.GetValue(key.GetData(), *result);
}
