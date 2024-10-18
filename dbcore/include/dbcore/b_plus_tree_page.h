#pragma once

#include <dbcore/coretypes.h>


namespace dbcore
{

enum class BPlusTreePageType { INVALID_TYPE = 0, LEAF_PAGE_TYPE, INTERNAL_PAGE_TYPE };

/**
 * Both internal and leaf page are inherited from this page.
 * 
 * It actually serves as a header part for each B+ tree page and
 * contains information required by both leaf page and internal page.
 * 
 * Header format (size in bytes, 16 bytes in total)
 * --------------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | KeySize (4) |.......... |
 * --------------------------------------------------------------------------
 * 
*/

class BPlusTreePage
{
    BPlusTreePage() = delete;
    BPlusTreePage(const BPlusTreePage&) = delete;
    BPlusTreePage& operator=(const BPlusTreePage&) = delete;
    ~BPlusTreePage() = delete;

public:
    bool IsLeafPage() const { return _page_type == BPlusTreePageType::LEAF_PAGE_TYPE; }
    // void SetPageType(BPlusTreePageType page_type) { _page_type = page_type; }

    uint32_t GetSize() const { return _size; }
    void SetSize(uint32_t size) { _size = size; }

    uint32_t GetMaxSize() const { return _max_size; }
    // void SetMaxSize(uint32_t max_size) { _max_size = max_size; }

    bool IsFull() const { return _size == _max_size; } 

protected:
    void Init(BPlusTreePageType page_type, uint32_t max_size, uint32_t key_size);
    uint32_t GetKeySize() const { return _key_size; }

private:
    BPlusTreePageType _page_type{BPlusTreePageType::INVALID_TYPE};
    /** the number of items in the node */
    uint32_t _size{0};
    /** the maximal number of items */
    uint32_t _max_size{0};
    /** the key length in bytes */
    uint32_t _key_size{0};
};

}