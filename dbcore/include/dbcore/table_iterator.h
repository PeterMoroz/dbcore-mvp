#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/rid.h>
#include <dbcore/tuple.h>

namespace dbcore
{

class TableHeap;
class PagesManager;

/**
 * TableIterator enables the sequential scan of a TableHeap.
*/
class TableIterator final
{
    TableIterator(const TableIterator&) = delete;
    TableIterator& operator=(const TableIterator&) = delete;

public:

    TableIterator(TableHeap &table_heap, const RID& rid, const RID& stop_at_rid);
    TableIterator(TableIterator&&) = default;

    // TO DO: documentation.
    std::pair<TupleMeta, Tuple> GetTuple() const;
    RID GetRID() const { return _rid; }
    bool IsEnd() const { return _rid.GetPageId() == INVALID_PAGE_ID; }

    void Next();

private:
    TableHeap& _table_heap;
    RID _rid;
    /** The ID of the last available record at the moment when iterator created. */
    RID _stop_at_rid;
};

}
