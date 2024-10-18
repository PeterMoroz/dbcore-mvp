#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/index.h>

#include <unordered_map>
#include <string>
#include <atomic>

namespace dbcore
{

class TableInfo;
class IndexInfo;
class Schema;
class PagesManager;

/**
 * The Catalog class represents non-persistent catalog.
 * It handles table creation, table lookup, index creation, index lookup.
 * TO DO: 
 *        1. think about persistency (how to store/load catalog on the page and/or on the RAMCloud). 
 *        2. go away from STL-containers and string.
 *        3. current implementaion is not thread-safe.
 *        4. think how to avoid raw pointers, use wrappers with reference counting, something else.
*/
class Catalog final
{
    Catalog(const Catalog&) = delete;
    Catalog& operator=(const Catalog&) = delete;

public:
    /**
     * Create a new catalog.
    */
    explicit Catalog(PagesManager* pages_manager);

    ~Catalog();

    /**
     * Create a new table and return pointer to table's metadata.
     * @param table_name the name of the new table
     * @param schema schema of the new table
     * @return A (non owning) pointer the table's metadata
    */
    TableInfo* CreateTable(const char* table_name, const Schema& schema);

    /**
     * Get table's metadata by table's OID.
     * @param oid The OID of the table to query
     * @return A (non owning) pointer to the table's metadata
    */
    TableInfo* GetTable(table_oid_t oid) const;

    /**
     * Get table's metadata by table's name.
     * @param name The name of the table to query
     * @return A (non owning) pointer to the table's info 
    */
    TableInfo* GetTable(const char* name) const;


    /**
     * Create a new index, populate existing data of the table 
     * and return the index metadata.
     * @param index_name teh name of the new index
     * @param table_name the name of the table
     * @param tbl_schema The schema of the table
     * @param key_attrbiutes The mapping of the table schema into key schema
     * @param num_of_key_attributes The number of attributes in the index key
     * @param index_type The type of the index
     * @return A (non owning) pointer to the index's info
    */
    IndexInfo* CreateIndex(const char* index_name, const char* table_name, const Schema& tbl_schema,
                        uint32_t key_attributes[], uint32_t num_of_key_attributes, IndexType index_type);

    /**
     * Get the index by its name and the table name.
     * @param index_name The name of the required index
     * @param table_name The name of the table on which index is built 
     * @return A (non owning) pointer to the index's info
    */
    IndexInfo* GetIndex(const char* index_name, const char* table_name);

    /**
     * Get the index by its name and the table name.
     * @param index_name The name of the required index
     * @param table_oid The OID of the table on which index is built 
     * @return A (non owning) pointer to the index's info
    */
    IndexInfo* GetIndex(const char* index_name, table_oid_t table_oid);

    /**
     * Get the index by its OID.
     * @param index_oid The OID of the required index
     * @return A (non owning) pointer to the index's info
    */
    IndexInfo* GetIndex(index_oid_t index_oid);


private:
    PagesManager* _pages_manager{nullptr};

    /** The next table identifier to be used */
    std::atomic<table_oid_t> _next_table_oid{0};

    /** Map table identifier -> table metadata.
     * The catalog is the owber of the TableInfo objects
     * and responsible for their destruction.
    */
    std::unordered_map<table_oid_t, TableInfo *> _tables;

    /**
     * Map table name -> table identifier
    */
    std::unordered_map<std::string, table_oid_t> _table_names;

    /** The next index identifier to be used */
    std::atomic<index_oid_t> _next_index_oid{0};

    /** Map index identifier -> index metadata.
     * The catalog is the owner of the IndexInfo objects 
     * and responsible for their destruction.
     */
    std::unordered_map<index_oid_t, IndexInfo *> _indexes;

    /** Map table name -> index names -> index identifiers */
    std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> _index_names;
};

}
