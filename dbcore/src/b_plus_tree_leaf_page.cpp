#include <dbcore/b_plus_tree_leaf_page.h>
#include <dbcore/rid.h>
#include <dbcore/tuple_compare.h>

#include <cassert>
#include <cstring>

#include <type_traits>

namespace dbcore
{
    static constexpr uint32_t BPLUS_LEAF_PAGE_VALUE_SIZE = sizeof(RID);

    static_assert(std::is_standard_layout<RID>::value == true);
    static_assert(std::is_trivially_copyable<RID>::value == true);
}

using namespace dbcore;


void BPlusTreeLeafPage::Init(uint32_t key_size)
{
    const uint32_t max_num_items = MaxNumItems(key_size);
    BPlusTreePage::Init(BPlusTreePageType::LEAF_PAGE_TYPE, max_num_items, key_size);
    ::memset(_data, 0, BPLUS_LEAF_PAGE_DATA_SIZE);
}

void BPlusTreeLeafPage::Init(uint32_t key_size, uint16_t max_size)
{
    const uint32_t max_num_items = MaxNumItems(key_size);
    assert(max_size < max_num_items);
    BPlusTreePage::Init(BPlusTreePageType::LEAF_PAGE_TYPE, max_size, key_size);
    _next_page_id = INVALID_PAGE_ID;
    ::memset(_data, 0, BPLUS_LEAF_PAGE_DATA_SIZE);
}

uint32_t BPlusTreeLeafPage::MaxNumItems(uint32_t key_size)
{
    return (BPLUS_LEAF_PAGE_DATA_SIZE / (key_size + BPLUS_LEAF_PAGE_VALUE_SIZE));
}

std::pair<bool, uint16_t> BPlusTreeLeafPage::FindItem(const char* key, const TupleCompare& key_cmp) const
{
    // const uint32_t max_num_of_items = GetMaxSize();

    if (GetSize() == 0)
    {
        return {false, 0};
    }

    const uint16_t pos = bsearch(key, key_cmp);
    if (pos < GetSize())
    {
        const char* key_at_pos = KeyAt(pos);
        const bool exist = (key_cmp(key, key_at_pos) == 0);
        return {exist, pos};
    }

    return {false, pos};
}

void BPlusTreeLeafPage::InsertAt(uint16_t pos, const char* key, const RID& rid)
{
    const uint16_t key_size = GetKeySize();
    const uint16_t num_items = GetSize();

    const uint32_t item_size = key_size + BPLUS_LEAF_PAGE_VALUE_SIZE;
    const uint16_t offset = pos * (item_size);

    if (pos < num_items) {
        // shift right, to free space for insertion
        uint16_t i = num_items;
        while (i > pos) {
            char* dst = _data + i * item_size;
            i--;
            char* src = _data + i * item_size;
            ::memcpy(dst, src, item_size);
        }
    }

    char* dst = _data + offset;
    ::memcpy(dst, key, key_size);
    dst += key_size;
    ::memcpy(dst, &rid, sizeof(rid));

    SetSize(GetSize() + 1);
}

void BPlusTreeLeafPage::RemoveAt(uint16_t pos)
{
    if (GetSize() == 0)
        return;
    
    const uint16_t shift_count = GetSize() - pos - 1;
    if (shift_count)
    {
        // shift left
        const uint16_t key_size = GetKeySize();
        const uint32_t item_size = key_size + BPLUS_LEAF_PAGE_VALUE_SIZE;
        const uint16_t offset = pos * (item_size);

        uint16_t i = 0;
        while (i < shift_count)
        {
            char* dst = _data + (i + pos) * item_size;
            i++;
            char* src = _data + (i + pos) * item_size;
            ::memcpy(dst, src, item_size);
        }
    }

    SetSize(GetSize() - 1);
}

void BPlusTreeLeafPage::Insert(const char* key, const RID& rid, const TupleCompare& key_cmp)
{
    const uint16_t key_size = GetKeySize();

    if (GetSize() == 0)
    {
        char* dst = _data;
        ::memcpy(dst, key, key_size);
        dst += key_size;
        ::memcpy(dst, &rid, sizeof(rid));
        SetSize(1);
        return;
    }

    const uint16_t pos_to_insert = bsearch(key, key_cmp);
    InsertAt(pos_to_insert, key, rid);
}

RID BPlusTreeLeafPage::GetValueAt(uint32_t pos) const
{
    RID rid;
    if (pos < GetSize())
    {
        const uint32_t key_size = GetKeySize();
        const uint32_t offset = pos * (key_size + sizeof(RID)) + key_size;
        const char* src = _data + offset;
        ::memcpy(&rid, src, sizeof(rid));
    }
    return rid;
}

void BPlusTreeLeafPage::CopyFrom(const BPlusTreeLeafPage* src_page, uint16_t start_pos)
{
    assert(start_pos < src_page->GetSize());
    const uint16_t num_items_to_copy = src_page->GetSize() - start_pos;

    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_LEAF_PAGE_VALUE_SIZE;

    const char* src = src_page->_data + start_pos * item_size;
    char* dst = _data;
    ::memcpy(dst, src, item_size * num_items_to_copy);

    SetSize(num_items_to_copy);
}

const char* BPlusTreeLeafPage::KeyAt(uint16_t pos) const
{
    assert(pos < GetMaxSize());
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_LEAF_PAGE_VALUE_SIZE;
    return _data + pos * (item_size);
}

uint16_t BPlusTreeLeafPage::bsearch(const char* key, const TupleCompare& key_cmp) const
{
    if (GetSize() == 1) {
        const char *key0 = KeyAt(0);
        if (key_cmp(key0, key) == 0) {
            return 0;
        }

        if (key_cmp(key0, key) == 1) {
            return 0;
        } else {
            return 1;
        }
    }

    uint16_t start = 0;
    uint16_t end = GetSize() - 1;
    while (start <= end) {
        const uint16_t mid = (start + end) / 2;
        const char *mkey = KeyAt(mid);
        if (key_cmp(mkey, key) == 0) {
            return mid;
        }
        if (key_cmp(mkey, key) == -1) {
            start = mid + 1;
        } else {
            if (mid == 0)
                return mid;
            end = mid - 1;
        }
    }
    return end + 1;
}

void BPlusTreeLeafPage::MergeRight(const BPlusTreeLeafPage* right_sibling)
{
    const uint16_t size = GetSize();
    const uint16_t right_size = right_sibling->GetSize();
    // sanity check
    if ((size > (GetMaxSize() / 2)) || (right_size > (right_sibling->GetMaxSize() / 2)))
    {
        return;
    }

    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_LEAF_PAGE_VALUE_SIZE;

    const char* src = right_sibling->_data;
    char* dst = _data + size * item_size;
    ::memcpy(dst, src, item_size * right_size);

    SetSize(size + right_size);

    SetNextPageId(right_sibling->GetNextPageId());
}
