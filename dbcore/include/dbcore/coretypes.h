#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>


namespace dbcore
{

// storage 
using page_id_t = int32_t;

static constexpr page_id_t INVALID_PAGE_ID = -1;

static constexpr uint32_t PAGE_SIZE = 16384;

static constexpr uint32_t MAX_COLUMN_COUNT = 32;


using slot_offset_t = uint16_t;

static constexpr slot_offset_t INVALID_SLOT_OFFSET = std::numeric_limits<slot_offset_t>::max();

static_assert(INVALID_SLOT_OFFSET >= PAGE_SIZE);

using slot_id_t = uint16_t;

static constexpr slot_id_t INVALID_SLOT_ID = std::numeric_limits<slot_offset_t>::max();

static_assert(INVALID_SLOT_ID >= PAGE_SIZE);


using table_oid_t = uint32_t;
using index_oid_t = uint32_t;

static constexpr table_oid_t INVALID_TABLE_OID = std::numeric_limits<table_oid_t>::max();
static constexpr index_oid_t INVALID_INDEX_OID = std::numeric_limits<index_oid_t>::max();

static constexpr size_t MAX_TABLE_NAME_SIZE = 64;
static constexpr size_t MAX_INDEX_NAME_SIZE = 64;

}