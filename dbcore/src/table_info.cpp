#include <dbcore/table_info.h>
#include <dbcore/table_heap.h>

#include <cstring>
#include <cstdlib>

using namespace dbcore;

TableInfo::TableInfo(const Schema& schema, const char* table_name,
                    TableHeap* table_heap, table_oid_t table_oid)
    : _schema(schema)
    , _table_heap(table_heap)
    , _table_oid(table_oid)
{
    ::memset(_table_name, 0, sizeof(_table_name));
    ::strncpy(_table_name, table_name, MAX_TABLE_NAME_SIZE);
}

TableInfo::~TableInfo()
{
    if (_table_heap)
    {
        _table_heap->~TableHeap();
        ::free(_table_heap);
        _table_heap = nullptr;
    }
}
