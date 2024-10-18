#pragma once

#include <dbcore/coretypes.h>
#include <cstdint>

namespace dbcore
{

class RID final
{
public:
    /** Default constructor creates invalid RID. */
    RID() = default;

    /**
     * Creates a RID (Record IDentifier) for the given page 
     * identifier and slot number. 
    */
    RID(page_id_t page_id, slot_id_t slot_id)
        : _page_id(page_id)
        , _slot_id(slot_id)
    {}

    page_id_t GetPageId() const { return _page_id; }
    slot_id_t GetSlotId() const { return _slot_id; }

    bool operator==(const RID& other) const { return _page_id == other._page_id && _slot_id == other._slot_id; }

private:
    page_id_t _page_id{INVALID_PAGE_ID};
    slot_id_t _slot_id{0};
};

}
