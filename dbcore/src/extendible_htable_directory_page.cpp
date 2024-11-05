#include <dbcore/extendible_htable_directory_page.h>

#include <cassert>

using namespace dbcore;

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth)
{
    assert(max_depth <= HTABLE_DIRECTORY_MAX_DEPTH);
    _max_depth = max_depth;
    _global_depth = 0;
    for (size_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
        _local_depths[i] = 0;
        _bucket_page_ids[i] = INVALID_PAGE_ID;
    }
}

uint32_t ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const
{
    static const uint32_t masks[] = { 0x00000000, 
        0x00000001, 0x00000003, 0x00000007, 0x0000000f, 
        0x0000001f, 0x0000003f, 0x0000007f, 0x000000ff,
        0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
        0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff,
        0x0001ffff, 0x0003ffff, 0x0007ffff, 0x000fffff,
        0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
        0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff,
        0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff
    };
    assert (_global_depth <= 32);
    return hash & masks[_global_depth];
}

page_id_t ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const
{
    assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
    return _bucket_page_ids[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id)
{
    assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
    _bucket_page_ids[bucket_idx] = bucket_page_id;
}

uint32_t ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const
{
    assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
    return _local_depths[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth)
{
    assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
    _local_depths[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth()
{
    assert(_global_depth < _max_depth);
    const uint32_t size = Size();
    uint32_t hi_idx = size;
    uint32_t lo_idx = 0;
    for (; lo_idx < size; lo_idx++, hi_idx++) {
        _local_depths[hi_idx] = _local_depths[lo_idx];
        _bucket_page_ids[hi_idx] = _bucket_page_ids[lo_idx];
    }
    _global_depth++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth()
{
    _global_depth--;
}

bool ExtendibleHTableDirectoryPage::CanShrink() const
{
    const uint32_t size = Size();
    if (size < 2) {
        return false;
    }

    const uint32_t half_size = size >> 1;
    uint32_t i = 0, j = half_size;
    for (; i < half_size && j < size; i++, j++) {
        if ((_local_depths[i] != _local_depths[j]) || (_bucket_page_ids[i] != _bucket_page_ids[j])) {
            return false;
        }
    }
    return true;
}

bool ExtendibleHTableDirectoryPage::VerifyIntegrity() const
{
    const uint32_t size = Size();

    // check all LD <= GD
    for (uint32_t i = 0; i < size; i++) {
        if (!(_local_depths[i] <= _global_depth)) {
            return false;
        }
    }

    // check that LD is the same at each index with the same bucket_page_id
    for (uint32_t i = 0; i < size - 1; i++) {
        const page_id_t bucket_page_id = _bucket_page_ids[i];
        const uint8_t ld = _local_depths[i];
        for (uint32_t j = i + 1; j < size; j++) {
            if (_bucket_page_ids[j] == bucket_page_id && _local_depths[j] != ld) {
                return false;
            }
        }
    }

    // check that each bucket has precisely 2^(GD - LD) pointers pointing to it
    for (uint32_t i = 0; i < size; i++) {
        const page_id_t bucket_page_id = _bucket_page_ids[i];
        const uint16_t expected_count = 1 << (_global_depth - _local_depths[i]);
        uint16_t actual_count = 0;
        for (uint32_t j = 0; j < size; j++) {
            if (_bucket_page_ids[j] == bucket_page_id) {
                actual_count++;
            }
        }

        if (actual_count != expected_count) {
            return false;
        }
    }

    return true;
}
