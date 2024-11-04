#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/schema.h>


namespace dbcore
{

class TableHeap;

class TableInfo final
{
    TableInfo(const TableInfo&) = delete;
    TableInfo& operator=(const TableInfo&) = delete;

public:
    TableInfo(const Schema& schema, const char* table_name,
            TableHeap* table_heap, table_oid_t table_oid);

    ~TableInfo();


    const char* GetTableName() const { return _table_name; }

    TableHeap* GetTableHeap() { return _table_heap; }

private:
    /** The table schema */
    Schema _schema;
    /** The table name */
    char _table_name[MAX_TABLE_NAME_SIZE + 1];
    /** The owning pointer to the table heap */
    TableHeap* _table_heap{nullptr};
    /** The table OID */
    table_oid_t _table_oid{INVALID_TABLE_OID};
};

}