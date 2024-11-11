#include <dbcore/extendible_htable_header_page.h>

#include <cassert>

using namespace dbcore;

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth)
{
    assert(max_depth <= HTABLE_HEADER_MAX_DEPTH);
    _max_depth = max_depth > 0 ? max_depth : HTABLE_HEADER_MAX_DEPTH;
    for (uint32_t i = 0; i < HTABLE_HEADER_ARRAY_SIZE; i++) {
        _directory_page_ids[i] = INVALID_PAGE_ID;
    }
}

uint32_t ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const
{
    if (_max_depth == 0) {
        return 0;
    }

    // use _max_depth high bits of hash as index in directory page
    return (hash >> ((sizeof(hash) << 3) - _max_depth));
}

page_id_t ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t idx) const
{
    return _directory_page_ids[idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t idx, page_id_t page_id)
{
    _directory_page_ids[idx] = page_id;
}

uint32_t ExtendibleHTableHeaderPage::MaxSize() const
{
    return (1 << _max_depth);
}
