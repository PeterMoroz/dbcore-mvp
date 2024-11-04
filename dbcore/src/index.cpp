#include <dbcore/index.h>
#include <dbcore/b_plus_tree_index.h>
#include <dbcore/tuple.h>
#include <dbcore/tuple_compare.h>

#include <cassert>
#include <algorithm>
#include <iterator>

using namespace dbcore;


IndexMetadata::IndexMetadata(uint32_t key_attrs[], uint32_t key_attrs_count, 
                            const Schema& key_schema, const Schema& tbl_schema)
    : _key_attrs_count(key_attrs_count)
    , _key_schema(key_schema)
    , _tbl_schema(tbl_schema)    
{
    assert(key_attrs_count < MAX_COLUMN_COUNT);
    std::copy_n(key_attrs, _key_attrs_count, _key_attrs.begin());
}


Index::Index(IndexType type, const IndexMetadata& metadata, PagesManager& pages_manager)
    : _type(type)
    , _metadata(metadata)
{
    switch (_type)
    {
    case IndexType::BPlusTreeIndex: {
        TupleCompare tuple_compare{_metadata.GetKeySchema()};
        _pimpl = static_cast<BPlusTreeIndex *>(::malloc(sizeof(BPlusTreeIndex)));
        new(_pimpl)BPlusTreeIndex(pages_manager, tuple_compare, _metadata.GetKeySchema().GetInlinedStorageSize());
        break;
    }
    default:
        assert(false); // not implemented or not supported
    }

}

Index::~Index()
{
    if (_pimpl)
    {
        switch (_type)
        {
        case IndexType::BPlusTreeIndex: {
            BPlusTreeIndex *index_impl = static_cast<BPlusTreeIndex *>(_pimpl);
            index_impl->~BPlusTreeIndex();
            break;
        }
        default:
            assert(false); // not implemented or not supported
        }

        ::free(_pimpl);
        _pimpl = nullptr;
    }
}

bool Index::InsertEntry(const Tuple& tuple, const RID& rid)
{
    assert(_pimpl);

    switch (_type)
    {
    case IndexType::BPlusTreeIndex: {
        BPlusTreeIndex *index_impl = static_cast<BPlusTreeIndex *>(_pimpl);
        const Tuple key{tuple.KeyFromTuple(_metadata.GetTableSchema(), _metadata.GetKeySchema(),
                        _metadata.GetKeyAttributes(), _metadata.GetKeyAttrCount())};
        return index_impl->InsertEntry(key, rid);
    }
    default:
        assert(false); // not implemented or not supported
    }

    return false;
}

void Index::DeleteEntry(const Tuple& tuple)
{
    assert(_pimpl);

    switch (_type)
    {
    case IndexType::BPlusTreeIndex: {
        BPlusTreeIndex *index_impl = static_cast<BPlusTreeIndex *>(_pimpl);
        const Tuple key{tuple.KeyFromTuple(_metadata.GetTableSchema(), _metadata.GetKeySchema(),
                        _metadata.GetKeyAttributes(), _metadata.GetKeyAttrCount())};        
        index_impl->DeleteEntry(key);
        break;
    }
    default:
        assert(false); // not implemented or not supported
    }

}

bool Index::SearchEntry(const Tuple& tuple, RID* result) const
{
    assert(_pimpl);

    switch (_type)
    {
    case IndexType::BPlusTreeIndex: {
        BPlusTreeIndex *index_impl = static_cast<BPlusTreeIndex *>(_pimpl);
        const Tuple key{tuple.KeyFromTuple(_metadata.GetTableSchema(), _metadata.GetKeySchema(),
                        _metadata.GetKeyAttributes(), _metadata.GetKeyAttrCount())};
        return index_impl->SearchEntry(key, result);
    }
    default:
        assert(false); // not implemented or not supported
    }

    return false;
}
