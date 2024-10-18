#include <dbcore/index_info.h>
#include <dbcore/index.h>

#include <cstring>
#include <cstdlib>

using namespace dbcore;

IndexInfo::IndexInfo(const Schema& schema, const char* name,
                    Index* index, const char* table_name,
                    index_oid_t index_oid)
    : _schema(schema)
    , _index(index)
    , _index_oid(index_oid)
{
    ::memset(_name, 0, sizeof(_name));
    ::strncpy(_name, _name, MAX_INDEX_NAME_SIZE);

    ::memset(_table_name, 0, sizeof(_table_name));
    ::strncpy(_table_name, table_name, MAX_TABLE_NAME_SIZE);   
}

IndexInfo::~IndexInfo()
{
    if (_index)
    {
        _index->~Index();
        ::free(_index);
        _index = nullptr;
    }
}
