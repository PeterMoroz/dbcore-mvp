#pragma once

#include <dbcore/tuple.h>

#include <cstdint>

#include <tuple>

namespace dbcore
{

class TablePage final
{
    TablePage(const TablePage&) = delete;
    TablePage& operator=(const TablePage&) = delete;

public:
    /**
     * Initialize the table page header.
    */
    void Init();

    /**
     * @return number of tuples in the page
    */
    uint16_t GetNumTuples() const { return _num_tuples; }

    /**
     * @return the page ID of the next table's page
    */
    page_id_t GetNextPageId() const { return _next_page_id; }

    /** set the page ID of the next table's page */
    void SetNextPageId(page_id_t next_page_id) { _next_page_id = next_page_id; }

    /** Get the next offset to insert, return INVALID_SLOT_OFFSET if the tuple can't fit in the page */
    slot_offset_t GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const;

    /**
     * Insert a tuple into the page.
     * @param tuple tuple to insert
     * @return slot_id if the insert is successfull, INVALID_SLOT_ID otherwise (ex. not enough space)
    */
    slot_id_t InsertTuple(const TupleMeta &meta, const Tuple &tuple);

    /**
     * Read a tuple from the page.
     * @param rid the ID of required tuple
     * @return the meta and tuple, when rid is not valid will return dummy tuple
    */ 
    std::pair<TupleMeta, Tuple> GetTuple(const RID& rid) const;

private:
    using TupleInfo = std::tuple<slot_offset_t, uint16_t, TupleMeta>;
    static constexpr size_t TUPLE_INFO_SIZE = sizeof(TupleInfo);
    static constexpr size_t TABLE_PAGE_HEADER_SIZE = sizeof(page_id_t) + sizeof(uint16_t) + sizeof(uint16_t);
    char _page_data[0];
    page_id_t _next_page_id{INVALID_PAGE_ID};
    uint16_t _num_tuples{0};
    uint16_t _num_deleted_tuples{0};
    TupleInfo _tuple_info[0];
};

}