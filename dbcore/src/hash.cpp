#include <dbcore/hash.h>

#include <cassert>

namespace dbcore
{

uint32_t dummy_hash(const void* data, size_t size)
{
    assert(size >= sizeof(uint32_t));
    return *static_cast<const uint32_t *>(data);
}

uint32_t FNV_hash(const void* data, size_t size)
{
    const uint8_t *p = static_cast<const uint8_t *>(data);
    uint32_t h = 2166136261;
    for (size_t i = 0; i < size; i++) {
        h = (h * 16777619) ^ p[i];
    }
    return h;
}

}
