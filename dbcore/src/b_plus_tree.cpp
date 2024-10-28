#include <dbcore/b_plus_tree.h>
#include <dbcore/pages_manager.h>
#include <dbcore/b_plus_tree_page.h>
#include <dbcore/b_plus_tree_internal_page.h>
#include <dbcore/b_plus_tree_leaf_page.h>

#include <dbcore/tuple_compare.h>
#include <dbcore/rid.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <limits>

using namespace dbcore;


BPlusTree::Iterator::Iterator(Iterator&& other)
    : _pages_manager(other._pages_manager)
    , _curr_page_id(other._curr_page_id)
    , _curr_pos(other._curr_pos)
    , _page_guard(std::move(other._page_guard))
{

}

// BPlusTree::Iterator& BPlusTree::Iterator::operator=(Iterator&& other)
// {
//     if (this != &other)
//     {
//         _page_guard.Drop();
//     }
//     return *this;
// }

BPlusTree::Iterator::Iterator(PagesManager& pages_manager, page_id_t start_page_id)
    : Iterator(pages_manager, start_page_id, 0)
{
}

BPlusTree::Iterator::Iterator(PagesManager& pages_manager, page_id_t start_page_id, uint16_t start_pos)
    : _pages_manager(pages_manager)
    , _curr_page_id(start_page_id)
    , _curr_pos(start_pos)
{
    if (_curr_page_id != INVALID_PAGE_ID) {
        _page_guard = _pages_manager.GetPageRead(_curr_page_id);
    }
    _curr_pos = 0;
}

bool BPlusTree::Iterator::IsEnd() const
{
    return _curr_page_id == INVALID_PAGE_ID;
}

RID BPlusTree::Iterator::operator*() const
{
    if (_curr_page_id != INVALID_PAGE_ID) {
        auto page = _page_guard.As<BPlusTreeLeafPage>();
        assert(page);
        return page->GetValueAt(_curr_pos);
    }
    assert(false);
    return RID{};
}

BPlusTree::Iterator& BPlusTree::Iterator::operator++()
{
    if (_curr_page_id != INVALID_PAGE_ID) {
        auto page = _page_guard.As<BPlusTreeLeafPage>();
        _curr_pos += 1;
        if (_curr_pos == page->GetSize()) {
            _curr_pos = 0;
            _curr_page_id = page->GetNextPageId();
            if (_curr_page_id != INVALID_PAGE_ID) {
                _page_guard = _pages_manager.GetPageRead(_curr_page_id);
            }
        }
    }
    return *this;
}

bool BPlusTree::Iterator::operator==(const Iterator& other) const
{
    return _curr_page_id == other._curr_page_id && _curr_pos == other._curr_pos;
}

bool BPlusTree::Iterator::operator!=(const Iterator& other) const
{
    return _curr_page_id != other._curr_page_id || _curr_pos != other._curr_pos;    
}


BPlusTree::BPlusTree(PagesManager& pages_manager, const TupleCompare& tuple_compare, uint32_t key_size,
                    uint16_t leaf_max_size, uint16_t internal_max_size)
    : _pages_manager(pages_manager)
    , _key_compare(tuple_compare)
    , _key_size(key_size)
    , _leaf_max_size(leaf_max_size)
    , _internal_max_size(internal_max_size)
{
    page_id_t root_page_id{INVALID_PAGE_ID};
    auto guard = _pages_manager.NextFreePageGuarded(&root_page_id);
    assert(root_page_id != INVALID_PAGE_ID);
    
    auto root_page = guard.AsMut<BPlusTreeLeafPage>();
    if (_leaf_max_size == 0)
        root_page->Init(key_size);
    else
        root_page->Init(key_size, _leaf_max_size);
    _root_page_id = root_page_id;
}

BPlusTree::~BPlusTree()
{
    if (_root_page_id != INVALID_PAGE_ID) {
        // std::cout << " ~BPlusTree() root_page_id = " << _root_page_id << std::endl;
        // TO DO: set the dirty flag if changes are made
        _pages_manager.UnpinPage(_root_page_id, false);
    }
}

bool BPlusTree::Insert(const char* key, const RID& rid)
{
    const uint32_t k = *reinterpret_cast<const uint32_t *>(key);
    
    // lock the whole tree now.
    // TO DO: think about locking with page granularity
    std::unique_lock lock(_mutex);

    assert(_root_page_id != INVALID_PAGE_ID);
    auto guard = _pages_manager.GetPageGuarded(_root_page_id);

    auto bplus_tree_page = guard.As<BPlusTreePage>();
    const bool is_leaf = bplus_tree_page->IsLeafPage();

    if (is_leaf) {
        /* when the root node is leaf, just insert key/value pair
         in case when split is happened, create a new root */
        auto bplus_leaf_page = guard.AsMut<BPlusTreeLeafPage>();

        page_id_t right_sibling_id{INVALID_PAGE_ID};
        if (!InsertIntoLeaf(bplus_leaf_page, key, rid, &right_sibling_id)) {
            return false;
        }

        // no split happened
        if (right_sibling_id == INVALID_PAGE_ID) {
            return true;
        }

        // otherwise, create new root
        page_id_t root_page_id{INVALID_PAGE_ID};
        auto guard = _pages_manager.NextFreePageGuarded(&root_page_id);
        assert(root_page_id != INVALID_PAGE_ID);
        
        auto root_page = guard.AsMut<BPlusTreeInternalPage>();
        if (_internal_max_size == 0)
            root_page->Init(_key_size);
        else
            root_page->Init(_key_size, _internal_max_size);
        // initial root page becomes the leftmost children
        const page_id_t left_page_id{_root_page_id};
        _root_page_id = root_page_id;

        root_page->SetValueAt(0, left_page_id);
        {
            auto right_page_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_page = right_page_guard.As<BPlusTreeLeafPage>();
            root_page->InsertAt(1, right_page->KeyAt(0), right_sibling_id);
        }
        return true;
    } else {
        /* when the root node is internal node, find the link to the children matching the key 
         and invoke helper method (which recursively traverse down to the lowest level)
        */
        auto bplus_internal_page = guard.AsMut<BPlusTreeInternalPage>();
        auto pos = bplus_internal_page->FindItem(key, _key_compare);
        auto child_page_id = bplus_internal_page->GetValueAt(pos);
        auto child_guard = _pages_manager.GetPageGuarded(child_page_id);
        auto child_page = child_guard.AsMut<BPlusTreeInternalPage>();

        page_id_t right_sibling_id{INVALID_PAGE_ID};
        if (!Insert(child_page, bplus_internal_page, key, rid, &right_sibling_id)) {
            return false;
        }

        // no split happened
        if (right_sibling_id == INVALID_PAGE_ID) {
            return true;
        }

        // otherwise, create new root
        page_id_t root_page_id{INVALID_PAGE_ID};
        auto guard = _pages_manager.NextFreePageGuarded(&root_page_id);
        assert(root_page_id != INVALID_PAGE_ID);
        
        auto root_page = guard.AsMut<BPlusTreeInternalPage>();
        if (_internal_max_size == 0)
            root_page->Init(_key_size);
        else
            root_page->Init(_key_size, _internal_max_size);
        // initial root page becomes the leftmost children
        const page_id_t left_page_id{_root_page_id};
        _root_page_id = root_page_id;

        root_page->SetValueAt(0, left_page_id);
        {
            auto right_page_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_page = right_page_guard.As<BPlusTreeInternalPage>();
            root_page->InsertAt(1, right_page->KeyAt(0), right_sibling_id);
        }

        return true;
    }

    // should be unreachable
    assert(false);
    return false;
}

void BPlusTree::Remove(const char* key)
{
    const uint32_t k = *reinterpret_cast<const uint32_t *>(key);

    // lock the whole tree now
    std::unique_lock lock(_mutex);

    if (_root_page_id == INVALID_PAGE_ID) {
        return;
    }

    // const uint64_t k = *reinterpret_cast<const uint64_t *>(key);

    auto guard = _pages_manager.GetPageGuarded(_root_page_id);

    auto bplus_tree_page = guard.As<BPlusTreePage>();
    const bool is_leaf = bplus_tree_page->IsLeafPage();
    if (is_leaf) {
        // when the root node is leaf, just remove key/value pair
        auto bplus_leaf_page = guard.AsMut<BPlusTreeLeafPage>();
        auto find_result = bplus_leaf_page->FindItem(key, _key_compare);
        if (!find_result.first) {
            return;
        }
        bplus_leaf_page->RemoveAt(find_result.second);
    } else {
        auto bplus_internal_page = guard.AsMut<BPlusTreeInternalPage>();
        Remove(bplus_internal_page, key);
        // the children were merged and only one remained (when internal node is zero size 
        // it means that it has only single value and no keys, if the tree in valid state)
        if (bplus_internal_page->GetSize() == 0) {
            const page_id_t child_id = bplus_internal_page->GetValueAt(0);
            assert(child_id != INVALID_PAGE_ID);
            _root_page_id = child_id;
        }
    }

    GiveBackDroppedPages();
}

bool BPlusTree::GetValue(const char* key, RID& value)
{
    // lock the whole tree now.
    // TO DO: think about locking with page granularity
    std::shared_lock lock(_mutex);

    if (_root_page_id == INVALID_PAGE_ID) {
        return false;
    }

    page_id_t page_id = _root_page_id;
    auto guard = _pages_manager.GetPageGuarded(page_id);
    auto bplus_tree_page = guard.As<BPlusTreePage>();
    while (!bplus_tree_page->IsLeafPage()) {
        auto bplus_internal_page = guard.As<BPlusTreeInternalPage>();
        const uint16_t pos = bplus_internal_page->FindItem(key, _key_compare);
        page_id = bplus_internal_page->GetValueAt(pos);
        guard = _pages_manager.GetPageGuarded(page_id);
        bplus_tree_page = guard.As<BPlusTreePage>();
    }

    auto bplus_leaf_page = guard.As<BPlusTreeLeafPage>();
    auto find_result = bplus_leaf_page->FindItem(key, _key_compare);

    if (find_result.first) {
        value = bplus_leaf_page->GetValueAt(find_result.second);
        return true;
    }
    return false;
}

BPlusTree::Iterator BPlusTree::Begin() const
{
    // ATTENTION!!
    // the implementation is not thread safe.
    // it is possible to acquire a lock of a whole tree, 
    // but iterator goes along the leaf pages which can only be protected individually
    assert(_root_page_id != INVALID_PAGE_ID);
    page_id_t page_id = _root_page_id;    
    auto read_guard = _pages_manager.GetPageRead(page_id);

    auto bplus_tree_page = read_guard.As<BPlusTreePage>();
    while (!bplus_tree_page->IsLeafPage()) {
        auto bplus_internal_page = read_guard.As<BPlusTreeInternalPage>();
        page_id = bplus_internal_page->GetValueAt(0);
        read_guard = _pages_manager.GetPageRead(page_id);
        bplus_tree_page = read_guard.As<BPlusTreePage>();
    }

    return std::move(Iterator(_pages_manager, page_id));    
}

BPlusTree::Iterator BPlusTree::Begin(const char* key) const
{
    assert(_root_page_id != INVALID_PAGE_ID);
    page_id_t page_id = _root_page_id;    
    auto read_guard = _pages_manager.GetPageRead(page_id);

    auto bplus_tree_page = read_guard.As<BPlusTreePage>();
    while (!bplus_tree_page->IsLeafPage()) {
        auto bplus_internal_page = read_guard.As<BPlusTreeInternalPage>();
        const uint16_t pos = bplus_internal_page->FindItem(key, _key_compare);
        page_id = bplus_internal_page->GetValueAt(pos);
        read_guard = _pages_manager.GetPageRead(page_id);
        bplus_tree_page = read_guard.As<BPlusTreePage>();
    }

    auto bplus_leaf_page = read_guard.As<BPlusTreeLeafPage>();
    auto find_result = bplus_leaf_page->FindItem(key, _key_compare);
    if (find_result.first) {
        return std::move(Iterator(_pages_manager, page_id, find_result.second));        
    }

    return std::move(Iterator(_pages_manager, INVALID_PAGE_ID));
}

BPlusTree::Iterator BPlusTree::End() const
{
    return std::move(Iterator(_pages_manager, INVALID_PAGE_ID));
}


bool BPlusTree::InsertIntoLeaf(BPlusTreeLeafPage* leaf, const char* key, const RID& rid, page_id_t* right_sibling)
{
    assert(leaf != nullptr);
    assert(right_sibling != nullptr);
    *right_sibling = INVALID_PAGE_ID;

    auto find_result = leaf->FindItem(key, _key_compare);
    // the key already present
    if (find_result.first) {
        return false;
    }

    if (!leaf->IsFull()) {
        leaf->InsertAt(find_result.second, key, rid);
        return true;
    }

    // split leaf in the middle
    page_id_t right_page_id{INVALID_PAGE_ID};
    auto guard = _pages_manager.NextFreePageGuarded(&right_page_id);
    assert(right_page_id != INVALID_PAGE_ID);
    
    auto right_page = guard.AsMut<BPlusTreeLeafPage>();
    if (_leaf_max_size == 0)
        right_page->Init(_key_size);
    else
        right_page->Init(_key_size, _leaf_max_size);

    if (leaf->GetSize() == 2) {
        const char* key1 = leaf->KeyAt(1);
        /* insert key/value into current leaf when key is strictly less
         than key at the split point of current leaf 
         otherwise - insert into the right sibling (just created) */
        const bool insert_into_left = (_key_compare(key, key1) == -1);

        right_page->CopyFrom(leaf, 1);
        leaf->SetSize(1);

        if (insert_into_left) {
            leaf->Insert(key, rid, _key_compare);
        } else {
            right_page->Insert(key, rid, _key_compare);
        }
    } else {
        const uint16_t mid_pos = leaf->GetSize() / 2;
        const char* mkey = leaf->KeyAt(mid_pos);
        /* insert key/value into current leaf when key is strictly less
          than key at the middle position of current leaf 
          otherwise - insert into the right sibling (just created) */
        const bool insert_into_left = (_key_compare(key, mkey) == -1);

        if (insert_into_left) {
            right_page->CopyFrom(leaf, mid_pos);
            leaf->SetSize(mid_pos);
            leaf->Insert(key, rid, _key_compare);
        } else {
            right_page->CopyFrom(leaf, mid_pos + 1);
            leaf->SetSize(mid_pos + 1);
            right_page->Insert(key, rid, _key_compare);
        }
    }

    // adjust sibling links
    const page_id_t next_page_id = leaf->GetNextPageId();
    leaf->SetNextPageId(right_page_id);
    right_page->SetNextPageId(next_page_id);

    *right_sibling = right_page_id;

    return true;
}

bool BPlusTree::Insert(BPlusTreePage* page, BPlusTreeInternalPage* parent, const char* key, const RID& rid, page_id_t* parent_right_sibling)
{
    if (page->IsLeafPage()) {
        auto bplus_leaf_page = static_cast<BPlusTreeLeafPage *>(page);
        page_id_t right_sibling_id{INVALID_PAGE_ID};
        if (!InsertIntoLeaf(bplus_leaf_page, key, rid, &right_sibling_id)) {
            return false;
        }

        // no split happened
        if (right_sibling_id == INVALID_PAGE_ID) {
            return true;
        }

        // otherwise, try to insert link into the parent. if parent is full - split it.
        if (!parent->IsFull()) {
            auto right_page_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_page = right_page_guard.As<BPlusTreeLeafPage>();
            const char* rkey0 = right_page->KeyAt(0);
            const uint16_t pos = parent->FindItem(rkey0, _key_compare);
            parent->InsertAt(pos + 1, rkey0, right_sibling_id);
            return true;
        }

        page_id_t parent_right_sibling_id{INVALID_PAGE_ID};
        auto guard = _pages_manager.NextFreePageGuarded(&parent_right_sibling_id);
        assert(parent_right_sibling_id != INVALID_PAGE_ID);

        auto right_page = guard.AsMut<BPlusTreeInternalPage>();
        if (_internal_max_size == 0)
            right_page->Init(_key_size);
        else
            right_page->Init(_key_size, _internal_max_size);

        const uint16_t midpos = parent->GetSize() / 2;
        right_page->CopyFrom(parent, midpos + 1);
        parent->SetSize(midpos);

        {
            auto right_sibling_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_sibling = right_sibling_guard.As<BPlusTreeLeafPage>();
            const char* leaf_rkey0 = right_sibling->KeyAt(0);
            const char* rkey0 = right_page->KeyAt(0);
            /* insert key/value into the left half of parent when key is strictly less
             than key at splitpoint, otherwise - insert into the right half */
            if (_key_compare(leaf_rkey0, rkey0) == -1) {
                parent->Insert(leaf_rkey0, right_sibling_id, _key_compare);
            } else {
                right_page->Insert(leaf_rkey0, right_sibling_id, _key_compare);
            }
        }
       
        assert(parent_right_sibling);
        *parent_right_sibling = parent_right_sibling_id;
        return true;
    } else {
        auto bplus_internal_page = static_cast<BPlusTreeInternalPage *>(page);
        const uint16_t pos = bplus_internal_page->FindItem(key, _key_compare);
        const page_id_t page_id = bplus_internal_page->GetValueAt(pos);

        auto child_page_guard = _pages_manager.GetPageGuarded(page_id);
        auto child_page = child_page_guard.AsMut<BPlusTreePage>();
        
        page_id_t right_sibling_id{INVALID_PAGE_ID};
        if (!Insert(child_page, bplus_internal_page, key, rid, &right_sibling_id)) {
            return false;
        }

        if (right_sibling_id == INVALID_PAGE_ID) {
            return true;
        }

        // the current page was split, try to insert link into parent.
        // when parent is full, also split it.
        if (!parent->IsFull()) {
            auto right_page_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_page = right_page_guard.As<BPlusTreeInternalPage>();
            const char* rkey0 = right_page->KeyAt(0);
            const uint16_t pos = parent->FindItem(rkey0, _key_compare);
            parent->InsertAt(pos + 1, rkey0, right_sibling_id);
            return true;
        }        

        page_id_t parent_right_sibling_id{INVALID_PAGE_ID};
        auto guard = _pages_manager.NextFreePageGuarded(&parent_right_sibling_id);
        assert(parent_right_sibling_id != INVALID_PAGE_ID);

        auto right_page = guard.AsMut<BPlusTreeInternalPage>();
        if (_internal_max_size == 0)
            right_page->Init(_key_size);
        else
            right_page->Init(_key_size, _internal_max_size);

        const uint16_t midpos = parent->GetSize() / 2;
        right_page->CopyFrom(parent, midpos + 1);
        parent->SetSize(midpos);

        {
            auto right_sibling_guard = _pages_manager.GetPageGuarded(right_sibling_id);
            auto right_sibling = right_sibling_guard.As<BPlusTreeInternalPage>();
            const char* child_rkey0 = right_sibling->KeyAt(0);
            const char* rkey0 = right_page->KeyAt(0);
            /* insert key/value into the left half of parent when key is strictly less
             than key at splitpoint, otherwise - insert into the right half */
            if (_key_compare(child_rkey0, rkey0) == -1) {
                parent->Insert(child_rkey0, right_sibling_id, _key_compare);
            } else {
                right_page->Insert(child_rkey0, right_sibling_id, _key_compare);
            }
        }
       
        assert(parent_right_sibling);
        *parent_right_sibling = parent_right_sibling_id;

        return true;
    }
    // should be unreachable
    assert(false);
    return false;
}

page_id_t BPlusTree::FindTheLeftmostChild(const BPlusTreePage* page)
{
    if (page->IsLeafPage())
        return INVALID_PAGE_ID;

    auto internal_page = static_cast<const BPlusTreeInternalPage *>(page);
    page_id_t child_id = internal_page->GetValueAt(0);
    while (1) {
        auto guard = _pages_manager.GetPageGuarded(child_id);
        const BPlusTreePage* child = guard.As<BPlusTreePage>();
        if (child->IsLeafPage())
            return child_id;
        internal_page = guard.As<BPlusTreeInternalPage>();
        child_id = internal_page->GetValueAt(0);
    }
    // shold be unreachable
    assert(false);
}

void BPlusTree::PrintTree(std::ostream& os) const
{
    os << " BPlusTree: "
        << " internal max size = " << _internal_max_size
        << " leaf max size = " << _leaf_max_size
        << " root page id = " << _root_page_id
        << "\n -------- " << std::endl;
    if (_root_page_id != INVALID_PAGE_ID) {
        auto guard = _pages_manager.GetPageGuarded(_root_page_id);
        auto page = guard.As<BPlusTreePage>();
        PrintTree(os, page, _root_page_id);
    }
    os << " -------------------------------- " << std::endl;
}

void BPlusTree::PrintTree(std::ostream& os, const BPlusTreePage* page, page_id_t page_id) const
{
    if (page->IsLeafPage()) {
        const BPlusTreeLeafPage* leaf = static_cast<const BPlusTreeLeafPage *>(page);
        os << "Leaf: page_id = " << page_id << "\tnext = " << leaf->GetNextPageId() << std::endl;
        os << "Contents: ";
        for (uint16_t i = 0; i < leaf->GetSize(); i++) {
            // os << leaf->KeyAt(i);
            const char* pkey = leaf->KeyAt(i);
            uint32_t key = *reinterpret_cast<const uint32_t *>(pkey);
            os << key;
            if ((i + 1) < leaf->GetSize())
                os << ", ";
        }
        os << '\n' << std::endl;
    } else {
        const BPlusTreeInternalPage* internal = static_cast<const BPlusTreeInternalPage *>(page);
        os << "Internal: page_id = " << page_id << std::endl;
        os << "Contents: ";
        for (uint16_t i = 0; i < internal->GetSize() + 1; i++) {
            // os << internal->KeyAt(i) << ": " << internal->GetValueAt(i);
            const char* pkey = internal->KeyAt(i);
            uint32_t key = *reinterpret_cast<const uint32_t *>(pkey);
            os << key << ": " << internal->GetValueAt(i);
            if (i < internal->GetSize())
                os << ", ";
        }
        os << '\n' << std::endl;
        for (uint16_t i = 0; i < internal->GetSize() + 1; i++) {
            page_id_t child_page_id = internal->GetValueAt(i);
            auto read_guard = _pages_manager.GetPageGuarded(child_page_id);
            auto child_page = read_guard.As<BPlusTreePage>();
            PrintTree(os, child_page, child_page_id);
        }
    }
}

void BPlusTree::GiveBackDroppedPages()
{
    for (const auto page_id : _dropped_pages) {
        if (!_pages_manager.GiveBackPage(page_id)) {
            std::cout << " give back page id " << page_id << " failed !" << std::endl;
        }
    }
    _dropped_pages.clear();
}

void BPlusTree::Remove(BPlusTreePage* page, const char* key)
{
    assert(!page->IsLeafPage());

    /* find the link to the children matching the key 
      and recursivelly traverse down to the lowest level
    */
    auto bplus_internal_page = static_cast<BPlusTreeInternalPage *>(page);
    const uint16_t pos = bplus_internal_page->FindItem(key, _key_compare);
    if (pos > bplus_internal_page->GetSize()) {
        return;
    }

    const page_id_t child_page_id = bplus_internal_page->GetValueAt(pos);
    auto child_guard = _pages_manager.GetPageGuarded(child_page_id);
    auto child_page = child_guard.AsMut<BPlusTreePage>();

    if (child_page->IsLeafPage()) {
        auto leaf_page = child_guard.AsMut<BPlusTreeLeafPage>();

        auto find_result = leaf_page->FindItem(key, _key_compare);
        if (!find_result.first) {
            return;
        }

        leaf_page->RemoveAt(find_result.second);
        // leaf is empty - adjust sibling links and remove link to it from the parent
        if (leaf_page->GetSize() == 0) {
            if (pos > 0) {
                const uint16_t left_pos = pos - 1;
                const page_id_t left_page_id = bplus_internal_page->GetValueAt(left_pos);
                auto left_guard = _pages_manager.GetPageGuarded(left_page_id);
                auto left_page = left_guard.AsMut<BPlusTreeLeafPage>();
                left_page->SetNextPageId(leaf_page->GetNextPageId());
            } else {

                page_id_t page_id = _root_page_id;
                auto guard = _pages_manager.GetPageGuarded(page_id);
                auto root_page = guard.As<BPlusTreePage>();
                page_id = FindTheLeftmostChild(root_page);

                if (page_id != child_page_id) {
                    while (1) {
                        guard = _pages_manager.GetPageGuarded(page_id);
                        auto page = guard.AsMut<BPlusTreeLeafPage>();
                        if (page->GetNextPageId() == child_page_id) {
                            page->SetNextPageId(leaf_page->GetNextPageId());
                            break;
                        }
                        page_id = page->GetNextPageId();
                    }
                }
            }
            bplus_internal_page->RemoveAt(pos);
            _dropped_pages.push_back(child_page_id);
            return;
        }

        // update key in the internal page when the leftmost key has gone
        if (find_result.second == 0) {
            bplus_internal_page->UpdateKeyAt(pos, leaf_page->KeyAt(0));
        }

        // merge with left or right sibling if possible
        if (leaf_page->GetSize() <= (leaf_page->GetMaxSize() / 2)) {
            const uint16_t right_pos = pos + 1;
            if (right_pos < bplus_internal_page->GetSize()) {
                const page_id_t right_page_id = bplus_internal_page->GetValueAt(right_pos);
                assert(right_page_id != INVALID_PAGE_ID);
                auto right_guard = _pages_manager.GetPageGuarded(right_page_id);
                auto right_page = right_guard.As<BPlusTreeLeafPage>();
                if (right_page->GetSize() <= (right_page->GetMaxSize() / 2)) {
                    leaf_page->MergeRight(right_page);
                    bplus_internal_page->RemoveAt(right_pos);
                    _dropped_pages.push_back(right_page_id);
                    return;
                }
            }

            if (pos > 0) {
                const uint16_t left_pos = pos - 1;
                const page_id_t left_page_id = bplus_internal_page->GetValueAt(left_pos);
                assert(left_page_id != INVALID_PAGE_ID);
                auto left_guard = _pages_manager.GetPageGuarded(left_page_id);
                auto left_page = left_guard.AsMut<BPlusTreeLeafPage>();
                if (left_page->GetSize() <= (left_page->GetMaxSize() / 2)) {
                    left_page->MergeRight(leaf_page);
                    bplus_internal_page->RemoveAt(pos);
                    _dropped_pages.push_back(child_page_id);
                    return;
                }
            }
        }

    } else {
        auto internal_page = child_guard.AsMut<BPlusTreeInternalPage>();
        Remove(internal_page, key);

        if (internal_page->GetSize() < (internal_page->GetMaxSize() / 2)) {
            // merge with left or right sibling
            const uint16_t right_pos = pos + 1;
            if (right_pos <= bplus_internal_page->GetSize()) {
                const page_id_t right_page_id = bplus_internal_page->GetValueAt(right_pos);
                assert(right_page_id != INVALID_PAGE_ID);
                auto right_guard = _pages_manager.GetPageGuarded(right_page_id);
                auto right_page = right_guard.AsMut<BPlusTreeInternalPage>();
                if ((right_page->GetSize() + internal_page->GetSize()) < internal_page->GetMaxSize()) {
                    // when merge, need to insert a key between. 
                    // it has to be the lowest key of the leftmostleaf of the sibling merged right     
                    const page_id_t lefmost_child_id = FindTheLeftmostChild(right_page);
                    assert(lefmost_child_id != INVALID_PAGE_ID);
                    auto leftmost_child_guard = _pages_manager.GetPageGuarded(lefmost_child_id);
                    auto leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                    // merge and update the key at the merge point
                    internal_page->MergeRight(right_page, leftmost_child->KeyAt(0));
                    bplus_internal_page->RemoveAt(right_pos);
                    _dropped_pages.push_back(right_page_id);
                    return;
                } else {
                    const uint16_t num_items_to_move = ((internal_page->GetMaxSize() / 2) - internal_page->GetSize());
                    if (num_items_to_move < right_page->GetSize() && (num_items_to_move <= right_page->GetMaxSize() / 2)) {
                        page_id_t lefmost_child_id = FindTheLeftmostChild(right_page);
                        assert(lefmost_child_id != INVALID_PAGE_ID);
                        auto leftmost_child_guard = _pages_manager.GetPageGuarded(lefmost_child_id);
                        auto leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                        internal_page->MoveFromRight(right_page, num_items_to_move, leftmost_child->KeyAt(0));

                        lefmost_child_id = FindTheLeftmostChild(right_page);
                        assert(lefmost_child_id != INVALID_PAGE_ID);
                        leftmost_child_guard = _pages_manager.GetPageGuarded(lefmost_child_id);
                        leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                        bplus_internal_page->UpdateKeyAt(right_pos, leftmost_child->KeyAt(0));
                        return;
                    }
                }
            }

            if (pos > 0) {
                const uint16_t left_pos = pos - 1;
                const page_id_t left_page_id = bplus_internal_page->GetValueAt(left_pos);
                assert(left_page_id != INVALID_PAGE_ID);
                auto left_guard = _pages_manager.GetPageGuarded(left_page_id);
                auto left_page = left_guard.AsMut<BPlusTreeInternalPage>();
                if ((left_page->GetSize() + internal_page->GetSize()) < left_page->GetMaxSize()) {
                    // when merge, need to insert a key between. 
                    // it has to be the lowest key of the leftmostleaf of the sibling merged right     
                    const page_id_t lefmost_child_id = FindTheLeftmostChild(internal_page);
                    assert(lefmost_child_id != INVALID_PAGE_ID);
                    auto leftmost_child_guard = _pages_manager.GetPageGuarded(lefmost_child_id);
                    auto leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                    // merge and update the key at the merge point
                    left_page->MergeRight(internal_page, leftmost_child->KeyAt(0));
                    bplus_internal_page->RemoveAt(pos);
                    _dropped_pages.push_back(child_page_id);
                    return;
                } else {
                    const uint16_t num_items_to_move = ((internal_page->GetMaxSize() / 2) - internal_page->GetSize());
                    if (num_items_to_move < left_page->GetSize() && (num_items_to_move <= left_page->GetMaxSize() / 2)) {
                        page_id_t leftmost_child_id = FindTheLeftmostChild(internal_page);
                        assert(leftmost_child_id != INVALID_PAGE_ID);
                        auto leftmost_child_guard = _pages_manager.GetPageGuarded(leftmost_child_id);
                        auto leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                        internal_page->MoveFromLeft(left_page, num_items_to_move, leftmost_child->KeyAt(0));

                        leftmost_child_id = FindTheLeftmostChild(internal_page);
                        assert(leftmost_child_id != INVALID_PAGE_ID);
                        leftmost_child_guard = _pages_manager.GetPageGuarded(leftmost_child_id);
                        leftmost_child = leftmost_child_guard.As<BPlusTreeLeafPage>();
                        bplus_internal_page->UpdateKeyAt(pos, leftmost_child->KeyAt(0));
                        return;                        
                    }
                }
            }

            if (internal_page->GetSize() == 0) {
                const uint64_t k = *reinterpret_cast<const uint64_t *>(key);
                std::cout << " ---- internal page size: " << internal_page->GetSize() 
                    << " page id = " << child_page_id << " pos = " << pos 
                    << " removed key = " << k << std::endl;
                // PrintTree(std::cout);
                assert(false);
            }
        }
    }
}
