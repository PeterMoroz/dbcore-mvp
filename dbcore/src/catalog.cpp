#include <dbcore/catalog.h>
#include <dbcore/table_heap.h>
#include <dbcore/table_info.h>
#include <dbcore/index.h>
#include <dbcore/index_info.h>

#include <cassert>
#include <cstdlib>

using namespace dbcore;

Catalog::Catalog(PagesManager* pages_manager)
    : _pages_manager(pages_manager)
{

}

Catalog::~Catalog()
{
    for (auto &[_, table] : _tables)
    {
        table->~TableInfo();
        ::free(table);
    }
}

TableInfo* Catalog::CreateTable(const char* table_name, const Schema& schema)
{
    const std::string tname(table_name);
    if (_table_names.count(tname)) {
        return nullptr;
    }

    TableHeap* table_heap = static_cast<TableHeap *>(::calloc(1, sizeof(TableHeap)));
    if (!table_heap) {
        return nullptr;
    }

    TableInfo* table_info = static_cast<TableInfo *>(::calloc(1, sizeof(TableInfo)));
    if (!table_info) {
        ::free(table_heap);
        return nullptr;
    }

    new (table_heap)TableHeap(*_pages_manager);

    const auto table_oid = _next_table_oid.fetch_add(1);

    new (table_info)TableInfo(schema, table_name, table_heap, table_oid);

    _tables.emplace(table_oid, table_info);
    _table_names.emplace(tname, table_oid);

    _index_names.emplace(tname, std::unordered_map<std::string, index_oid_t>());

    return table_info;
}

TableInfo* Catalog::GetTable(table_oid_t oid) const
{
    const auto it = _tables.find(oid);
    return it != _tables.cend() ? it->second : nullptr;
}

TableInfo* Catalog::GetTable(const char* name) const
{
    const std::string tname(name);
    const auto it = _table_names.find(tname);
    if (it == _table_names.cend()) {
        return nullptr;
    }

    const auto it2 = _tables.find(it->second);
    if (it2 == _tables.cend()) {
        assert(false);  // broken invariant !
        return nullptr;
    }

    return it2->second;
}

IndexInfo* Catalog::CreateIndex(const char* index_name, const char* table_name, const Schema& tbl_schema,
                                uint32_t key_attributes[], uint32_t num_of_key_attributes, IndexType index_type)
{
    const std::string idx_name(index_name);
    const std::string tbl_name(table_name);

    // reject creation indexes for nonexistent table
    if (_table_names.find(tbl_name) == _table_names.cend()) {
        return nullptr;
    }

    auto it_indexes = _index_names.find(tbl_name);
    // if the table exist, an entry for the table should already present
    if (it_indexes == _index_names.cend()) {
        assert(false);  // broken invariant !
        return nullptr;
    }

    auto& table_indexes = it_indexes->second;
    // check if the index with such name is already exist
    if (table_indexes.find(idx_name) != table_indexes.cend()) {
        return nullptr;
    }

    Schema key_schema{Schema::CopySchema(tbl_schema, key_attributes, num_of_key_attributes)};
    IndexMetadata meta(key_attributes, num_of_key_attributes, key_schema, tbl_schema);

    Index *index = static_cast<Index *>(::malloc(sizeof(Index)));
    if (!index) {
        return nullptr;
    }

    IndexInfo* index_info = static_cast<IndexInfo *>(::malloc(sizeof(IndexInfo)));
    if (!index_info) {
        ::free(index);
        return nullptr;
    }

    new(index)Index(index_type, meta, *_pages_manager);


    TableInfo* table_info = GetTable(table_name);
    assert(table_info != nullptr);

    TableHeap* table_heap = table_info->GetTableHeap();
    assert(table_heap != nullptr);

    auto itr = table_heap->MakeIterator(); 
    while (!itr.IsEnd()) {
        const auto [_, tuple] = itr.GetTuple();
        const RID rid{itr.GetRID()};
        index->InsertEntry(tuple, rid);
        itr.Next();
    }

    const auto index_oid = _next_index_oid.fetch_add(1);

    new(IndexInfo)(key_schema, index_name, index, table_name, index_oid);

    _indexes.emplace(index_oid, index_info);
    table_indexes.emplace(index_name, index_oid);

    return index_info;
}

IndexInfo* Catalog::GetIndex(const char* index_name, const char* table_name)
{
    const std::string idx_name(index_name);
    const std::string tbl_name(table_name);

    const auto it = _index_names.find(tbl_name);
    if (it == _index_names.cend()) {
        return nullptr;
    }
    
    const auto table_indexes = it->second;
    const auto it2 = table_indexes.find(idx_name);
    if (it2 == table_indexes.cend()) {
        return nullptr;
    }

    const auto it3 = _indexes.find(it2->second);
    if (it3 == _indexes.cend()) {
        assert(false);  // broken invariant !
        return nullptr;
    }

    return it3->second;
}

IndexInfo* Catalog::GetIndex(const char* index_name, table_oid_t table_oid)
{
    const auto it_tbl = _tables.find(table_oid);
    if (it_tbl == _tables.cend()) {
        return nullptr;
    }
    return GetIndex(index_name, it_tbl->second->GetTableName());
}

IndexInfo* Catalog::GetIndex(index_oid_t index_oid)
{
    const auto it = _indexes.find(index_oid);
    return it != _indexes.cend() ? it->second : nullptr;
}
