#pragma once

#include <dbcore/rid.h>

namespace dbcore
{

class IndexIterator final
{
public:
    IndexIterator() = default;
    ~IndexIterator() = default;

    bool IsEnd() const;

    const RID& operator*() const;

    IndexIterator& operator++();

    bool operator==(const IndexIterator& other) const;
    bool operator!=(const IndexIterator& other) const;

};

}
