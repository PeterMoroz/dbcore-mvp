#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/b_plus_tree_page.h>


namespace dbcore
{

class TupleCompare;

/** 
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy: K(i) <= K(i+1).
 * Note: Since the number of keys does not equal to number of child pointers, the first
 * key always remains invalid. That is to say, any search/lookup should ignore that one.
 * 
 * Internal page format (keys are stored in increasing order)
 * ----------------------------------------------------------------------------------
 * | HEADER | KEY(1) + PAGE_ID(1) | KEY(2) + PAGE_ID(2) | ... | KEY(n) + PAGE_ID(n) |
 * ----------------------------------------------------------------------------------
*/
class BPlusTreeInternalPage : public BPlusTreePage
{
    BPlusTreeInternalPage() = delete;
    BPlusTreeInternalPage(const BPlusTreeInternalPage&) = delete;
    BPlusTreeInternalPage& operator=(const BPlusTreeInternalPage&) = delete;
    ~BPlusTreeInternalPage() = delete;

public:
    void Init(uint32_t key_size);
    void Init(uint32_t key_size, uint16_t max_size);
    
    /**
     * Get value at the given position (index).
     * @param pos index of value to retrieve
     * @return the value at the given index or INVALID_PAGE_ID if index is out of bound
    */
    page_id_t GetValueAt(uint32_t pos) const;

    /**
     * Write value at the given position (index).
     * @param pos index at where write value
     * @param value the value to write
    */
    void SetValueAt(uint32_t pos, page_id_t value);

    /**
     * Insert the key/value pair at the specified position.
     * Assume that preconditions are met:
     * - the pos is less than max_size
     * - the node is not full (i.e. size < max_size)
     * The number of items will be increased by one after insertion.
     * @param pos position to insert
     * @param key key to insert
     * @param page_id value matching the key
    */
    void InsertAt(uint16_t pos, const char* key, page_id_t page_id);

    /**
     * Remove the key/value pair at the specified position.
     * @param pos position at which remove the key/value pair.
    */
    void RemoveAt(uint16_t pos);    

    /**
     * Insert the key value pair. Assume that node is not full.
     * The number of items will be increased by one after insertion.
     * @param key key to insert
     * @param page_id_t value matching the key
     * @param key_cmp keys' comparator
    */
    void Insert(const char* key, page_id_t page_id, const TupleCompare& key_cmp);

    /**
     * Search item by the given key.
     * @param key the key to search
     * @param key_cmp keys' comparator
    */
    uint16_t FindItem(const char* key, const TupleCompare& key_cmp) const;

    /**
     * Copy the key/value pairs from the given leaf page.
     * The pairs are copied starting from the given position up to 
     * the end of the source page. The pairs will be writen at the 
     * beginning of this node and the number of items in the node
     * will be set accordingly.
    */
    void CopyFrom(const BPlusTreeInternalPage* src_page, uint16_t start_pos);    

    /**
     * get pointer to key of item at specified position
    */
    const char* KeyAt(uint16_t pos) const;

    /**
     * update the key value at specified position
    */
    void UpdateKeyAt(uint16_t pos, const char* key);

    /**
     * Merge the sibling at the right side into the current page.
    */
    void MergeRight(const BPlusTreeInternalPage* right_sibling);

    /**
     * Merge the sibling at the right side into the current page. Insert the key between merged items.
    */
    void MergeRight(const BPlusTreeInternalPage* right_sibling, const char* key);

    /**
     * Move some number of items from the right sibling into the current page.
    */
    void MoveFromRight(BPlusTreeInternalPage* right_sibling, uint16_t num_items, const char* key);

    /**
     * Move some number of items from the left sibling into the current page.
    */
    void MoveFromLeft(BPlusTreeInternalPage* left_sibling, uint16_t num_items, const char* key);


    /**
     * Merge the sibling at the left side into the current page.
    */
    void MergeLeft(const BPlusTreeInternalPage* left_sibling);


private:
    uint16_t bsearch(const char* key, const TupleCompare& key_cmp) const;

    static uint32_t MaxNumItems(uint32_t key_size);

    static constexpr uint32_t BPLUS_INTERNAL_PAGE_HEADER_SIZE = sizeof(BPlusTreePage);
    static constexpr uint32_t BPLUS_INTERNAL_PAGE_DATA_SIZE = (PAGE_SIZE - BPLUS_INTERNAL_PAGE_HEADER_SIZE);

private:
    char _data[0];
};

}