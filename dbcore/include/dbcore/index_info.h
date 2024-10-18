#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/schema.h>

namespace dbcore
{

class Index;

class IndexInfo final
{
    IndexInfo(const IndexInfo&) = delete;
    IndexInfo& operator=(const IndexInfo&) = delete;

public:
    IndexInfo(const Schema& schema, const char* name,
            Index* index, const char* table_name,
            index_oid_t index_oid);

    ~IndexInfo();

private:
    /** The schema for the index key */
    Schema _schema;
    /** The name of the index */
    char _name[MAX_INDEX_NAME_SIZE + 1];
    /** An owning pointer to the index */
    Index* _index{nullptr};
    /** The name of the table on which the index is created */
    char _table_name[MAX_TABLE_NAME_SIZE + 1];
    /** The unique OID of the index */
    index_oid_t _index_oid{INVALID_INDEX_OID};
};

}
