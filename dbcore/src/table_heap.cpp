#include <dbcore/table_heap.h>
#include <dbcore/page_guard.h>
#include <dbcore/table_page.h>

using namespace dbcore;

TableHeap::TableHeap(PagesManager& pages_manager)
    : _pages_manager(pages_manager)
{
    page_id_t page_id = INVALID_PAGE_ID;
    // initialize the first table's page
    auto page_guard = _pages_manager.NextFreePageGuarded(&page_id);
    TablePage* table_page = page_guard.AsMut<TablePage>();
    table_page->Init();
    _first_page_id = _last_page_id = page_id;
}

RID TableHeap::InsertTuple(const TupleMeta &meta, const Tuple &tuple)
{
    // only allow one insertion at a time, otherwise it will deadlock.
    std::unique_lock lock(_mutex);
    WritePageGuard page_guard = _pages_manager.GetPageWrite(_last_page_id);
    // find the page suitable to insert the tuple
    while (true) {
        TablePage* page = page_guard.AsMut<TablePage>();
        if (page->GetNextTupleOffset(meta, tuple) != INVALID_SLOT_OFFSET) {
            break;
        }

        page_id_t next_page_id = INVALID_PAGE_ID;
        PageGuard npg = _pages_manager.NextFreePageGuarded(&next_page_id);
        // TO DO: check that next_page_id != INVALID_PAGE_ID, i.e. the valid page was got

        page->SetNextPageId(next_page_id);
        TablePage* next_page = npg.AsMut<TablePage>();
        next_page->Init();

        // Explicit Drop() is not needed, the moving assign operator will do it.
        /* page_guard.Drop(); */
        _last_page_id = next_page_id;
        page_guard = npg.UpgradeWrite();
    }

    const page_id_t last_page_id = _last_page_id;
    TablePage* page = page_guard.AsMut<TablePage>();

    slot_id_t slot_id = page->InsertTuple(meta, tuple);
    if (slot_id == INVALID_SLOT_ID) {
        return RID{};
    }

    // Explicit Drop() is not needed, the d-tor will do it.
    /* page_guard.Drop(); */

    return RID{last_page_id, slot_id};
}

TableIterator TableHeap::MakeIterator()
{
    page_id_t last_page_id = INVALID_PAGE_ID;
    {
        std::unique_lock lock(_mutex);
        last_page_id = _last_page_id;
    }

    auto page_guard = _pages_manager.GetPageRead(last_page_id);
    const TablePage* page = page_guard.As<TablePage>();
    const uint16_t num_tuples = page->GetNumTuples();
    return {*this, {_first_page_id, 0}, {_last_page_id, num_tuples}};
}

std::pair<TupleMeta, Tuple> TableHeap::GetTuple(const RID& rid) const
{
    if (rid.GetPageId() == INVALID_PAGE_ID) {
        // will return dummy tuple.
        return std::make_pair(TupleMeta{}, Tuple{});
    }

    auto page_guard = _pages_manager.GetPageRead(rid.GetPageId());
    const TablePage* page = page_guard.As<TablePage>();
    // page is responsible for handle situation when rid.slot_num is out of page's range
    auto [meta, tuple] = page->GetTuple(rid);
    return std::make_pair(meta, std::move(tuple));
}
