#include <dbcore/index.h>

#include <cassert>
#include <algorithm>
#include <iterator>

using namespace dbcore;


IndexMetadata::IndexMetadata(uint32_t key_attrs[], uint32_t key_attrs_count, const Schema& schema)
    : _schema(schema)
    , _key_attrs_count(key_attrs_count)
{
    assert(key_attrs_count < MAX_COLUMN_COUNT);
    std::copy_n(key_attrs, _key_attrs_count, _key_attrs.begin());
}


Index::Index(IndexType type, const IndexMetadata& metadata)
    : _type(type)
    , _metadata(metadata)
{

}

Index::~Index()
{
    assert(false);
}

bool Index::InsertEntry(const Tuple& key, const RID& rid)
{
    return false;
}

void Index::DeleteEntry(const Tuple& key)
{

}

bool Index::SearchEntry(const Tuple& key, RID* result) const
{
    return false;
}
