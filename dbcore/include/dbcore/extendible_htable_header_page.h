#pragma once

#include <dbcore/coretypes.h>

namespace dbcore
{

/**
 * HeaderPage format:
 *  -------------------------------------------------------------------
 * | MaxDepth(4) | DirectoryPageIDs(2048) | Free (depends on pagesize) |
 *  -------------------------------------------------------------------
*/

class ExtendibleHTableHeaderPage final
{
    ExtendibleHTableHeaderPage(const ExtendibleHTableHeaderPage&) = delete;
    ExtendibleHTableHeaderPage& operator=(const ExtendibleHTableHeaderPage&) = delete;

public:

    /**
     * Initialize a new header page after creation with pages manger.
     * @param max_depth - max depth of the header page
    */
    void Init(uint32_t max_depth);

    /**
     * Get the directory index that the key is hashed to.
     * @param hash - the hash of the key
     * @return directory index the key is hashed to
    */
    uint32_t HashToDirectoryIndex(uint32_t hash) const;

    /**
     * Get the directory page id at index
     * @param idx - index in the directory page id array
     * @return directory page id at index
    */
    page_id_t GetDirectoryPageId(uint32_t idx) const;

    /**
     * Set the directory page id at index
     * @param idx - index in the directory page id array
     * @param page_id - page id of the directory page
    */
    void SetDirectoryPageId(uint32_t idx, page_id_t page_id);

    /**
     * Get the maximum number of directory pages the header page could comprise
    */
    uint32_t MaxSize() const;


private:
    static constexpr uint16_t HTABLE_HEADER_PAGE_META_SIZE = sizeof(uint32_t);
    static constexpr uint16_t HTABLE_HEADER_MAX_DEPTH = 9;  // enough at first time. TO DO: revise later, taking into consideration PAGE_SIZE
    static constexpr uint16_t HTABLE_HEADER_ARRAY_SIZE = 1 << HTABLE_HEADER_MAX_DEPTH;

private:
    uint32_t _max_depth{0};
    page_id_t _directory_page_ids[HTABLE_HEADER_ARRAY_SIZE];
};

static_assert(sizeof(page_id_t) == 4);
static_assert(sizeof(ExtendibleHTableHeaderPage) <= PAGE_SIZE);

}
