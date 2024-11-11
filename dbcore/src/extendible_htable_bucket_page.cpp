#include <dbcore/extendible_htable_bucket_page.h>
#include <dbcore/rid.h>
#include <dbcore/tuple_compare.h>

#include <cassert>
#include <cstring>

#include <type_traits>

namespace dbcore
{
    static constexpr uint32_t HTABLE_BUCKET_VALUE_SIZE = sizeof(RID);

    static_assert(std::is_standard_layout<RID>::value == true);
    static_assert(std::is_trivially_copyable<RID>::value == true);
}

using namespace dbcore;

void ExtendibleHTableBucketPage::Init(uint32_t key_size)
{
    _key_size = key_size;
    _num_items = 0;
    _max_num_items = MaxNumItems(key_size);

    ::memset(_data, 0, HTABLE_BUCKET_PAGE_DATA_SIZE);
}

void ExtendibleHTableBucketPage::Init(uint32_t key_size, uint32_t max_num_items)
{
    Init(key_size);
    
    assert(max_num_items < MaxNumItems(key_size));
    _max_num_items = max_num_items;
}

uint32_t ExtendibleHTableBucketPage::MaxNumItems(uint32_t key_size)
{
    return (HTABLE_BUCKET_PAGE_DATA_SIZE / (key_size + HTABLE_BUCKET_VALUE_SIZE));
}

bool ExtendibleHTableBucketPage::Insert(const char* key, const TupleCompare& cmp, const RID& rid)
{
    const uint32_t item_size = _key_size + sizeof(RID);

    if (_num_items < _max_num_items) {
        if (_num_items == 0) {
            char* dst = _data;
            ::memcpy(dst, key, _key_size);
            dst += _key_size;
            ::memcpy(dst, &rid, sizeof(RID));
            _num_items++;
            return true;
        }

        // test and try first
        const char *key_first = _data;
        int cmp_result = cmp(key, key_first);
        if (cmp_result == 0) {
            return false;   // already exist
        } else if (cmp_result == -1) {
            uint32_t idx = _num_items;
            while (idx > 0) {
                char* dst = _data + idx * item_size;
                idx--;
                char* src = _data + idx * item_size;
                ::memcpy(dst, src, item_size);
            }
            char* dst = _data;
            ::memcpy(dst, key, _key_size);
            dst += _key_size;
            ::memcpy(dst, &rid, sizeof(RID));
            _num_items++;
            return true;
        }

        // test and try last
        const char *key_last = _data + (_num_items - 1) * item_size;
        cmp_result = cmp(key, key_last);
        if (cmp_result == 0) {
            return false;   // already exist
        } else if (cmp_result == 1) {
            char* dst = _data + _num_items * item_size;
            ::memcpy(dst, key, _key_size);
            dst += _key_size;
            ::memcpy(dst, &rid, sizeof(RID));
            _num_items++;
            return true;
        }

        uint32_t pos_to_insert = _num_items;
        if (bsearch(key, cmp, &pos_to_insert)) {
            return false;
        } else {
            uint32_t idx = _num_items;
            while (idx > pos_to_insert) {
                char* dst = _data + idx * item_size;
                idx--;
                char* src = _data + idx * item_size;
                ::memcpy(dst, src, item_size);
            }
            char* dst = _data + pos_to_insert * item_size;
            ::memcpy(dst, key, _key_size);
            dst += _key_size;
            ::memcpy(dst, &rid, sizeof(RID));
            _num_items++;
            return true;
        }
    }
    return false;
}

bool ExtendibleHTableBucketPage::Remove(const char* key, const TupleCompare& cmp)
{
    const uint32_t item_size = _key_size + sizeof(RID);

    if (_num_items != 0) {
        // check the first item
        const char *first_key = _data;
        if (cmp(key, first_key) == 0) {
            _num_items--;
            uint32_t idx = 0;
            while (idx < _num_items) {
                char* dst = _data + idx * item_size;
                idx++;
                char* src = _data + idx * item_size;
                ::memcpy(dst, src, item_size);
            }
            return true;
        }

        // check the last item
        const char *last_key = _data + (_num_items - 1) * item_size;
        if (cmp(key, last_key) == 0) {
            _num_items--;
            return true;
        }

        uint32_t pos_to_remove = _num_items;
        if (bsearch(key, cmp, &pos_to_remove)) {
            _num_items--;
            uint32_t idx = pos_to_remove;
            while (idx < _num_items) {
                char* dst = _data + idx * item_size;
                idx++;
                char* src = _data + idx * item_size;
                ::memcpy(dst, src, item_size);
            }
            return true;
        }
    }
    return false;
}

void ExtendibleHTableBucketPage::RemoveAt(uint32_t idx)
{
    if (_num_items > 0 && idx < _num_items) {
        const uint32_t item_size = _key_size + sizeof(RID);
        uint32_t i = idx;
        while (i < _num_items - 1) {
            char* dst = _data + i * item_size;
            i += 1;
            const char* src = _data + i * item_size;
            ::memcpy(dst, src, item_size);
        }
        _num_items--;
    }
}

bool ExtendibleHTableBucketPage::Lookup(const char* key, const TupleCompare& cmp, RID& rid) const
{
    const uint32_t item_size = _key_size + sizeof(RID);

    if (_num_items != 0) {
        uint32_t pos = _num_items;
        if (bsearch(key, cmp, &pos)) {
            const char* src = _data + pos * item_size + _key_size;
            ::memcpy(&rid, src, sizeof(rid));
            return true;
        }
    }
    return false;
}

const char* ExtendibleHTableBucketPage::KeyAt(uint32_t idx) const
{
    const uint32_t item_size = _key_size + sizeof(RID);
    const uint32_t offset = item_size * idx;
    const char* key = _data + offset;
    return key;
}

RID ExtendibleHTableBucketPage::ValueAt(uint32_t idx) const
{
    RID rid;
    const uint32_t item_size = _key_size + sizeof(RID);
    const uint32_t offset = item_size * idx;
    const char* value = _data + offset + _key_size;
    ::mempcpy(&rid, value, sizeof(RID));
    return rid;
}

bool ExtendibleHTableBucketPage::bsearch(const char* key, const TupleCompare& cmp, uint32_t* pos) const
{
    const uint32_t item_size = _key_size + sizeof(RID);
    uint32_t l = 0, r = _num_items - 1;
    while (l <= r) {
        uint32_t m = l + (r - l) / 2;
        const char* key_at_pos = _data + m * item_size;
        const int cmp_result = cmp(key, key_at_pos);
        if (cmp_result == 0) {
            *pos = m;
            return true;
        }
        if (cmp_result == 1) {
            if (m < _num_items - 1) {
                l = m + 1;
            } else {
                break;
            }
        } else {
            if (m > 0) {
                r = m - 1;
            } else {
                break;
            }
        }
    }
    *pos = l;
    return false;
}


/////////////////////////////////////////////////////////////////////////
// TO DO: remove. these are auxiliary stuff
//
#include <iostream>

std::ostream& operator<<(std::ostream& os, const RID& rid)
{
    os << " { " << rid.GetPageId() << ", " << rid.GetSlotId() << " } ";
    return os;
}

void ExtendibleHTableBucketPage::Print() const
{
    const uint32_t item_size = _key_size + sizeof(RID);
    uint32_t i = 0;
    for (; i < _num_items; i++) {
        const char* item = _data + i * item_size;
        std::cout << "item #i ";
        uint64_t key = 0xFFFFFFFF;
        ::memcpy(&key, item, sizeof(key));
        std::cout << key;
        RID rid;
        item += _key_size;
        ::memcpy(&rid, item, sizeof(rid));
        std::cout << rid << std::endl;
    }
}
