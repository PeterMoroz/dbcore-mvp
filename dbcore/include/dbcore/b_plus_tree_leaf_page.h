#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/b_plus_tree_page.h>

#include <utility>

namespace dbcore
{

class RID;
class TupleCompare;

/**
 * Store indexed key and record id together within leaf page. Only support unique key.
 * 
 * Leaf page format (keys are stored in order):
 * 
 * ------------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) |  ...  | KEY(n) + RID(n) |
 * ------------------------------------------------------------------------
 * 
 * Header format (size in bytes, 20 bytes in total)
 * -------------------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | KeySize (4) | NextPageId (4) |
 * -------------------------------------------------------------------------------
*/
class BPlusTreeLeafPage : public BPlusTreePage
{
    BPlusTreeLeafPage() = delete;
    BPlusTreeLeafPage(const BPlusTreeLeafPage&) = delete;
    BPlusTreeLeafPage& operator=(const BPlusTreeLeafPage&) = delete;
    ~BPlusTreeLeafPage() = delete;

public:
    void Init(uint32_t key_size);
    void Init(uint32_t key_size, uint16_t max_size);

    page_id_t GetNextPageId() const { return _next_page_id; }
    void SetNextPageId(page_id_t next_page_id) { _next_page_id = next_page_id; }

    /**
     * Get value at the given position (index).
     * @param pos index of value to retrieve
     * @return the value at the given index or invalid RID if index is out of bound
    */
    RID GetValueAt(uint32_t pos) const;


    /**
     * Search item by the given key.
     * @param key the key to search
     * @param key_cmp keys' comparator
     * @return the pair, the first element means presence of item with such key
     * the second one is position of element or position to insert if key is not found
    */
    std::pair<bool, uint16_t> FindItem(const char* key, const TupleCompare& key_cmp) const;

    /**
     * Insert the key/value pair at the specified position.
     * The number of items will be increased by one after insertion.
     * @param pos position to insert
     * @param key key to insert
     * @param rid value matching the key
    */
    void InsertAt(uint16_t pos, const char* key, const RID& rid);

    /**
     * Remove the key/value pair at the specified position.
     * @param pos position at which remove the key/value pair.
    */
    void RemoveAt(uint16_t pos);

    /**
     * Insert the key value pair. Assume that node is not full.
     * The number of items will be increased by one after insertion.
     * @param key key to insert
     * @param rid value matching the key
     * @param key_cmp keys' comparator
    */
    void Insert(const char* key, const RID& rid, const TupleCompare& key_cmp);

    /**
     * Copy the key/value pairs from the given leaf page.
     * The pairs are copied starting from the given position up to 
     * the end of the source page. The pairs will be writen at the 
     * beginning of this node and the number of items in the node
     * will be set accordingly.
    */
    void CopyFrom(const BPlusTreeLeafPage* src_page, uint16_t start_pos);

    /**
     * get pointer to key of item at specified position
    */
    const char* KeyAt(uint16_t pos) const;

    /**
     * Merge the sibling at the right side into the current page.
    */
    void MergeRight(const BPlusTreeLeafPage* right_sibling);

private:
    /**
     * Find the position to insert pair with given key using binary search.
     * @param key key to search
     * @param key_cmp keys' comparator
     * @return position to insert the pair with a given key
    */
    uint16_t bsearch(const char* key, const TupleCompare& key_cmp) const;

    static uint32_t MaxNumItems(uint32_t key_size);

    static constexpr uint32_t BPLUS_LEAF_PAGE_HEADER_SIZE = sizeof(BPlusTreePage) + sizeof(page_id_t);
    static constexpr uint32_t BPLUS_LEAF_PAGE_DATA_SIZE = (PAGE_SIZE - BPLUS_LEAF_PAGE_HEADER_SIZE);
   
private:
    page_id_t _next_page_id{INVALID_PAGE_ID};
    char _data[0];
};

}
