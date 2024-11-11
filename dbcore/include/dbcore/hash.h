#pragma once

#include <dbcore/coretypes.h>

namespace dbcore
{

uint32_t dummy_hash(const void* data, size_t size);

uint32_t FNV_hash(const void* data, size_t size);

}
