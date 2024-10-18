#pragma once

#include <dbcore/coretypes.h>

namespace dbcore
{

class ExtendibleHTableHeaderPage final
{
    ExtendibleHTableHeaderPage(const ExtendibleHTableHeaderPage&) = delete;
    ExtendibleHTableHeaderPage& operator=(const ExtendibleHTableHeaderPage&) = delete;

public:

    /**
     * Initialize a new header page after creation with pages manger.
     * @param max_depth the max depth of the header page
    */
    void Init(uint32_t max_depth);

    /**
     * Get the directory index that the key is hashed to.
    */
    uint32_t HashToDirectoryIndex(uint32_t hash) const;


private:
    static constexpr uint32_t HTABLE_HEADER_PAGE_META_SIZE = sizeof(uint32_t);
    static constexpr uint32_t HTABLE_HEADER_MAX_DEPTH = 9;  // enough at first time. TO DO: revise later, taking into consideration PAGE_SIZE
    static constexpr uint32_t HTABLE_HEADER_ARRAY_SIZE = 1 << HTABLE_HEADER_MAX_DEPTH;

private:
    uint32_t _max_depth{0};
    page_id_t _direcory_page_ids[HTABLE_HEADER_ARRAY_SIZE];
};

static_assert(sizeof(page_id_t) == 4);
static_assert(sizeof(ExtendibleHTableHeaderPage) <= PAGE_SIZE);

}
