#include <dbcore/table_iterator.h>
#include <dbcore/table_heap.h>
#include <dbcore/pages_manager.h>
#include <dbcore/table_page.h>

#include <cassert>

using namespace dbcore;


TableIterator::TableIterator(TableHeap &table_heap, const RID& rid, const RID& stop_at_rid)
    : _table_heap(table_heap)
    , _rid(rid)
    , _stop_at_rid(stop_at_rid)
{
    // set the END condition when the current RID is not valid for the first page
    if (_rid.GetPageId() != INVALID_PAGE_ID) {
        auto page_guard = _table_heap._pages_manager.GetPageRead(_rid.GetPageId());
        const TablePage* page = page_guard.As<TablePage>();
        if (_rid.GetSlotId() >= page->GetNumTuples()) {
            _rid = RID{INVALID_PAGE_ID, 0};
        }
    }
}

std::pair<TupleMeta, Tuple> TableIterator::GetTuple() const
{
    return _table_heap.GetTuple(_rid);
}

void TableIterator::Next()
{
    // don't check that current RID is valid in Release build.
    // assume that client code follow the contract and check precondition (!END) itself.
    assert(_rid.GetPageId() != INVALID_PAGE_ID);

    auto page_guard = _table_heap._pages_manager.GetPageRead(_rid.GetPageId());
    const TablePage* page = page_guard.As<TablePage>();
    const uint16_t next_tuple_id = _rid.GetSlotId() + 1;

#ifndef NDEBUG
    // sanity check
    if (_stop_at_rid.GetPageId() != INVALID_PAGE_ID)
    {          
        assert(
            /* cursor before the page of the stop-tuple */
            _rid.GetPageId() < _stop_at_rid.GetPageId() ||
            /* cursor at the page, but before the stop-tuple */
            (_rid.GetPageId() == _stop_at_rid.GetPageId() && next_tuple_id <= _stop_at_rid.GetSlotId())
        );
    }
#endif

    _rid = RID{_rid.GetPageId(), next_tuple_id};
    if (_rid == _stop_at_rid) {
        _rid = RID{INVALID_PAGE_ID, 0};
        return;
    }

    if (next_tuple_id >= page->GetNumTuples()) {
        const page_id_t next_page_id = page->GetNextPageId();
        // when no more pages, the next_page_id will be INVALID_PAGE_ID,
        // so the _rid will be assigned the terminal value
        _rid = RID{next_page_id, 0};
        return;
    }
}
