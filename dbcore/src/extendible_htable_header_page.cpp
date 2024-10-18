#include <dbcore/extendible_htable_header_page.h>

#include <cassert>

using namespace dbcore;

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth)
{
    assert(max_depth <= HTABLE_HEADER_MAX_DEPTH);
    _max_depth = max_depth;
    for (uint32_t i = 0; i < HTABLE_HEADER_ARRAY_SIZE; i++) {
        _direcory_page_ids[i] = INVALID_PAGE_ID;
    }
}

uint32_t ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const
{
    if (_max_depth == 0) {
        return 0;
    }

    // use _max_depth high bits of hash as index in direcotry page
    return (hash >> ((sizeof(hash) << 3) - _max_depth));
}
