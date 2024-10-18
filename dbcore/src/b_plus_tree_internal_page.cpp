#include <dbcore/b_plus_tree_internal_page.h>
#include <dbcore/tuple_compare.h>

#include <cassert>
#include <cstring>

namespace dbcore
{
    static constexpr uint32_t BPLUS_INTERNAL_PAGE_VALUE_SIZE = sizeof(page_id_t);
}

using namespace dbcore;


void BPlusTreeInternalPage::Init(uint32_t key_size)
{
    const uint32_t max_num_items = MaxNumItems(key_size);
    BPlusTreePage::Init(BPlusTreePageType::INTERNAL_PAGE_TYPE, max_num_items, key_size);
    ::memset(_data, 0, BPLUS_INTERNAL_PAGE_DATA_SIZE);
}

void BPlusTreeInternalPage::Init(uint32_t key_size, uint16_t max_size)
{
    const uint32_t max_num_items = MaxNumItems(key_size);
    assert(max_size < max_num_items);
    BPlusTreePage::Init(BPlusTreePageType::INTERNAL_PAGE_TYPE, max_size, key_size);
    ::memset(_data, 0, BPLUS_INTERNAL_PAGE_DATA_SIZE);
}

uint32_t BPlusTreeInternalPage::MaxNumItems(uint32_t key_size)
{
    return (BPLUS_INTERNAL_PAGE_DATA_SIZE / (key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE));
}

page_id_t BPlusTreeInternalPage::GetValueAt(uint32_t pos) const
{
    page_id_t page_id{INVALID_PAGE_ID};
    // if (pos < GetSize())
    // {
    //     const uint32_t key_size = GetKeySize();
    //     const uint32_t offset = pos * (key_size + sizeof(page_id_t)) + key_size;
    //     const char* src = _data + offset;
    //     ::memcpy(&page_id, src, sizeof(page_id));
    // }
    const uint32_t key_size = GetKeySize();
    const uint32_t offset = pos * (key_size + sizeof(page_id_t)) + key_size;
    const char* src = _data + offset;
    ::memcpy(&page_id, src, sizeof(page_id));
    return page_id;
}

void BPlusTreeInternalPage::SetValueAt(uint32_t pos, page_id_t value)
{
    if (pos < GetMaxSize())
    {
        const uint32_t key_size = GetKeySize();
        const uint32_t offset = pos * (key_size + sizeof(page_id_t)) + key_size;
        char* dst = _data + offset;
        ::memcpy(dst, &value, sizeof(value));
    }
}

void BPlusTreeInternalPage::InsertAt(uint16_t pos, const char* key, page_id_t page_id)
{
    assert(pos != 0);   // not applicable when pos == 0, use SetValue instead !
    const uint16_t key_size = GetKeySize();
    const uint16_t num_items = GetSize();

    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    const uint16_t offset = pos * (item_size);

    if (pos <= num_items) {
        // shift right, to free space for insertion
        uint16_t i = num_items + 1;
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
    ::memcpy(dst, &page_id, sizeof(page_id));

    SetSize(GetSize() + 1);
}

void BPlusTreeInternalPage::RemoveAt(uint16_t pos)
{
    if (GetSize() == 0)
        return;
    
    // const uint16_t shift_count = GetSize() - pos - 1;
    const uint16_t shift_count = GetSize() - pos;
    if (shift_count)
    {
        // shift left
        const uint16_t key_size = GetKeySize();
        const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
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

void BPlusTreeInternalPage::Insert(const char* key, page_id_t page_id, const TupleCompare& key_cmp)
{
    const uint16_t key_size = GetKeySize();

    if (GetSize() == 0)
    {
        char* dst = _data;
        ::memcpy(dst, key, key_size);
        dst += key_size;
        ::memcpy(dst, &page_id, sizeof(page_id));
        SetSize(1);
        return;
    }

    const uint16_t pos_to_insert = bsearch(key, key_cmp);
    InsertAt(pos_to_insert + 1, key, page_id);
}

uint16_t BPlusTreeInternalPage::FindItem(const char* key, const TupleCompare& key_cmp) const
{
    assert(GetSize() > 0);

    const uint16_t pos = bsearch(key, key_cmp);
    // if (pos < GetMaxSize())
    // {
    //     // const char* key_at_pos = KeyAt(pos);
    //     // const bool exist = (key_cmp(key, key_at_pos) == 0);
    //     return pos;
    // }
    // return 0;
    return pos;
}

void BPlusTreeInternalPage::CopyFrom(const BPlusTreeInternalPage* src_page, uint16_t start_pos)
{
    assert(start_pos < src_page->GetSize());
    const uint16_t num_items_to_copy = src_page->GetSize() - start_pos + 1;

    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;

    const char* src = src_page->_data + start_pos * item_size;
    char* dst = _data;
    ::memcpy(dst, src, item_size * num_items_to_copy);

    SetSize(num_items_to_copy - 1);
}

const char* BPlusTreeInternalPage::KeyAt(uint16_t pos) const
{
    // assert(pos != 0 && pos < GetMaxSize());
    // assert(pos < GetMaxSize());
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    return _data + pos * (item_size);
}

void BPlusTreeInternalPage::UpdateKeyAt(uint16_t pos, const char* key)
{
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    char* dst = _data + pos * (item_size);
    ::memcpy(dst, key, key_size);
}

void BPlusTreeInternalPage::MergeRight(const BPlusTreeInternalPage* right_sibling)
{
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    const uint16_t pos = GetSize() + 1;
    char* dst = _data + pos * (item_size);
    const char* src = right_sibling->_data;
    const uint16_t num_items = right_sibling->GetSize() + 1;
    ::memcpy(dst, src, num_items * item_size);
    SetSize(GetSize() + num_items);
}

void BPlusTreeInternalPage::MergeRight(const BPlusTreeInternalPage* right_sibling, const char* key)
{
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    const uint16_t pos = GetSize() + 1;
    char* dst = _data + pos * (item_size);
    const char* src = right_sibling->_data;
    // ::memcpy(dst, key, key_size);
    // dst += key_size;
    // const uint16_t num_items = right_sibling->GetSize() + 1;
    // ::memcpy(dst, src + key_size, num_items * item_size);

    const uint16_t num_items = right_sibling->GetSize() + 1;
    ::memcpy(dst, src, num_items * item_size);
    ::memcpy(dst, key, key_size);

    SetSize(GetSize() + num_items);
}

void BPlusTreeInternalPage::MoveFromRight(BPlusTreeInternalPage* right_sibling, uint16_t num_items, const char* key)
{
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    const uint16_t pos = GetSize() + 1;
    char* dst = _data + pos * (item_size);
    const char* src = right_sibling->_data;
    ::memcpy(dst, src, num_items * item_size);
    ::memcpy(dst, key, key_size);

    SetSize(GetSize() + num_items);

    src = right_sibling->_data + num_items * item_size;
    dst = right_sibling->_data;
    const uint16_t n = right_sibling->GetSize() + 1 - num_items;
    for (uint16_t i = 0; i < n; i++) {
        ::memcpy(dst, src, item_size);
        src += item_size;
        dst += item_size;
    }

    right_sibling->SetSize(right_sibling->GetSize() - num_items);
}

void BPlusTreeInternalPage::MoveFromLeft(BPlusTreeInternalPage* left_sibling, uint16_t num_items, const char* key)
{
    const uint16_t key_size = GetKeySize();
    const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    const uint16_t pos = GetSize() + 1;

    const char* src = _data + GetSize() * item_size;
    char* dst = const_cast<char *>(src) + num_items * item_size;
    for (uint16_t i = 0; i < GetSize() + 1; i++) {
        ::memcpy(dst, src, item_size);
        dst -= item_size;
        src -= item_size;
    }

    dst = _data;
    src = left_sibling->_data + ((left_sibling->GetSize() - num_items) + 1) * item_size;
    ::memcpy(dst, src, num_items * item_size);

    dst = _data + item_size * num_items;
    ::memcpy(dst, key, key_size);

    SetSize(GetSize() + num_items);
    left_sibling->SetSize(left_sibling->GetSize() - num_items);
}


void BPlusTreeInternalPage::MergeLeft(const BPlusTreeInternalPage* left_sibling)
{
    assert(false);  // not implemented
    // const uint16_t key_size = GetKeySize();
    // const uint32_t item_size = key_size + BPLUS_INTERNAL_PAGE_VALUE_SIZE;
    // const uint16_t pos = GetSize() + 1;
    // char* dst = _data + pos * (item_size);
    // const char* src = left_sibling->_data;
    // const uint16_t num_items = left_sibling->GetSize() + 1;
    // ::memcpy(dst, src, num_items * item_size);
    // SetSize(GetSize() + num_items);
}

uint16_t BPlusTreeInternalPage::bsearch(const char* key, const TupleCompare& key_cmp) const
{
    if (GetSize() == 1) {
        const char *key1 = KeyAt(1);
        if (key_cmp(key1, key) == 0) {
            return 1;
        }

        if (key_cmp(key1, key) == 1) {
            return 0;
        } else {
            return 1;
        }
    }

    uint16_t start = 1;
    // uint16_t end = GetSize() - 1;
    uint16_t end = GetSize();
    while (start <= end) {
        const uint16_t mid = (start + end) / 2;
        // if (mid == end)
        //     return mid;
        const char *mkey = KeyAt(mid);
        if (key_cmp(mkey, key) == 0) {
            return mid;
        }
        if (key_cmp(mkey, key) == -1) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return end;
}
