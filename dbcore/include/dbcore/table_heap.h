#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/tuple.h>
#include <dbcore/rid.h>
#include <dbcore/pages_manager.h>
#include <dbcore/table_iterator.h>

#include <mutex>

namespace dbcore
{

/**
 * TableHeap represents a table on some kind of storage (memory, disk, etc)
*/
class TableHeap final
{
    TableHeap(const TableHeap&) = delete;
    TableHeap& operator=(const TableHeap&) = delete;

public:
    /**
     * Create a table heap.
    */
    explicit TableHeap(PagesManager& pages_manager);

    /**
     * Insert a tuple into the table. If the tuple is too large (>= page_size), return invalid RID.
     * @param meta tuple meta
     * @param tuple tuple to insert
     * @return rid of the inserted tuple
    */
    RID InsertTuple(const TupleMeta &meta, const Tuple &tuple);

    /**
     * Create a table iterator instance.
     * When the iterator is created it will bounded at the end by 
     * the last tuple in the table heap and iterator will stop at that point.
     * @return the iterator of the table
    */
    // TO DO: create iterator is "logically const" opersation, fix compilation error.
    // TableIterator MakeIterator() const;
    TableIterator MakeIterator();

    /**
     * Read a tuple from the table.
     * @param rid the ID of required tuple
     * @return the meta and tuple, when rid is not valid will return dummy tuple
    */
    std::pair<TupleMeta, Tuple> GetTuple(const RID& rid) const;

private:
    PagesManager& _pages_manager;

    page_id_t _first_page_id{INVALID_PAGE_ID};
    /** The mutex ensure exclusive access to the @ref _last_page_id field */
    std::mutex _mutex;
    page_id_t _last_page_id{INVALID_PAGE_ID};

    friend class TableIterator; // friendship to access to _pages_manager field only!
};


}