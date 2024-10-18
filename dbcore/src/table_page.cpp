#include <dbcore/table_page.h>

#include <cstring>

using namespace dbcore;


void TablePage::Init()
{
    _next_page_id = INVALID_PAGE_ID;
    _num_tuples = 0;
    _num_deleted_tuples = 0;
}

slot_offset_t TablePage::GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const
{
    uint32_t slot_end_offset = _num_tuples > 0 ? std::get<0>(_tuple_info[_num_tuples - 1]) : PAGE_SIZE;
    if (slot_end_offset < tuple.GetLength()) {
        return INVALID_SLOT_OFFSET;
    }

    const uint32_t tuple_offset = slot_end_offset - tuple.GetLength();
    const uint32_t offset_size = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (_num_tuples + 1);
    if (tuple_offset < offset_size) {
        return INVALID_SLOT_OFFSET;
    }
    return tuple_offset;
}

slot_id_t TablePage::InsertTuple(const TupleMeta &meta, const Tuple &tuple)
{
    auto tuple_offset = GetNextTupleOffset(meta, tuple);
    if (tuple_offset == INVALID_SLOT_OFFSET) {
        return INVALID_SLOT_ID;
    }

    const uint16_t tuple_id = _num_tuples;
    _tuple_info[tuple_id] = std::make_tuple(tuple_offset, tuple.GetLength(), meta);
    ::memcpy(_page_data + tuple_offset, tuple.GetData(), tuple.GetLength());
    _num_tuples++;
    return tuple_id;
}

std::pair<TupleMeta, Tuple> TablePage::GetTuple(const RID& rid) const
{
    const uint16_t tuple_id = rid.GetSlotId();
    if (tuple_id >= _num_tuples) {
        return std::make_pair(TupleMeta{}, Tuple{});
    }

    const auto& [offset, size, meta] = _tuple_info[tuple_id];
    Tuple tuple(_page_data + offset, size, rid);
    return std::make_pair(meta, std::move(tuple));
}
