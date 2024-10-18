#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/page_guard.h>

#include <shared_mutex>

#include <iostream>

namespace dbcore
{

class PagesManager;
class RID;
class TupleCompare;

class BPlusTreePage;
class BPlusTreeLeafPage;
class BPlusTreeInternalPage;

class BPlusTree final
{
    BPlusTree(const BPlusTree&) = delete;
    BPlusTree& operator=(const BPlusTree&) = delete;

public:

    class Iterator final
    {
    public:
        Iterator() = delete;    // don't allow default construction. only BPlusTree class is able to construct the iterator.
        ~Iterator() = default;

        Iterator(Iterator&& other);
        // Iterator& operator=(Iterator&& other);

    private:
        Iterator(PagesManager& pages_manager, page_id_t start_page_id);
        Iterator(PagesManager& pages_manager, page_id_t start_page_id, uint16_t start_pos);

    public:
        bool IsEnd() const;

        RID operator*() const;

        Iterator& operator++();

        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

    private:
        PagesManager& _pages_manager;
        page_id_t _curr_page_id;
        uint16_t _curr_pos;
        // iterator instance is lock the page which is currently scanned
        ReadPageGuard _page_guard;

        friend class BPlusTree;
    };

public:
    BPlusTree(PagesManager& pages_manager, const TupleCompare& tuple_compare, uint32_t key_size, 
            uint16_t leaf_max_size = 0, uint16_t internal_max_size = 0);

    ~BPlusTree();

    /**
     * Insert a key-value pair into the tree.
    */
    bool Insert(const char* key, const RID& rid);

    /**
     * Remove a key and its value from the tree.
    */
    void Remove(const char* key);

    /**
     * Search and return a value associated with a given key
    */
    bool GetValue(const char* key, RID& rid);


    void PrintTree(std::ostream& os) const;

    /**
     * Return iterator at the beginning of the tree.
    */
    Iterator Begin() const;

    /**
     * Return iterator at the key/value pair specified by the given key.
     * When key is not found, return iterator at the end of the tree.
    */
    Iterator Begin(const char* key) const;

    /**
     * Return iterator at the end of the tree.
    */
    Iterator End() const;

private:

    /**
     * Insert a key-value pair into leaf node (The caller is responsible that it would be leaf node).
     * When leaf is full it will be split into two (in the middle), sibling links will be adjusted
     * and the page_id of the right node will be returned to the caller via parameter @ref right_sibling
     * @param leaf the pointer to leaf at which key-value pair inserted
     * @param key the key to insert into leaf
     * @param rid the value matching the key
     * @param right_sibling the pointer where page_id of the right sibling will be written in case of split, INVALID_PAGE_ID otherwise
     * @return true when pair was inserted, false when pair with such key is already exist.
    */
    bool InsertIntoLeaf(BPlusTreeLeafPage* leaf, const char* key, const RID& rid, page_id_t* right_sibling);

    /**
     * Helper method to insert key/value pair. When the first argument is leaf page, insert key/value into it. If leaf is split,
     * insert a link into parent page (always internal page). In case when parent is split, pass the page id of its right sibling
     * to the caller via the @ref parent_right_sibling argument. When the first argument is internal page, lookup the link to the
     * children matching the key and invoke itself recursively.
     * @param page the page where insert the key/value pair or lookup the link to traverse down
     * @param parent the parent of the @ref page (always internal page, because leaf can't be parent)
     * @param key the key to insert
     * @param rid the value to insert
     * @param parent_right_sibling the page_id of the parent's right sibling will be written here in case of parent split
     * @return true when the pair was inserted, false when the pair with such key already exist.
    */
    bool Insert(BPlusTreePage* page, BPlusTreeInternalPage* parent, const char* key, const RID& rid, page_id_t* parent_right_sibling);

    /**
     * Helper method to remove key/value pair.
     * @param page the page where remove the key/value pair or lookup the link to traverse down
     * @param key the key to remove
    */
    void Remove(BPlusTreePage* page, const char* key);

    /**
     * Traverse down following the leftmost children. When the next children is leaf, returns its ID
    */
    page_id_t FindTheLeftmostChild(const BPlusTreePage* page);

private:
    void PrintTree(std::ostream& os, const BPlusTreePage* page, page_id_t page_id) const;


private:
    PagesManager& _pages_manager;
    const TupleCompare& _key_compare;
    uint32_t _key_size{0};
    page_id_t _root_page_id{INVALID_PAGE_ID};
    uint16_t _leaf_max_size{0};
    uint16_t _internal_max_size{0};
    std::shared_mutex _mutex;
};

}
