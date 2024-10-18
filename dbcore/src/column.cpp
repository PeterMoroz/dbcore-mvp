#include <dbcore/column.h>

#include <cassert>
#include <cstring>

using namespace dbcore;

uint8_t Column::TypeSize(TypeId type, uint32_t length /*= 0*/)
{
    switch (type)
    {
        case TypeId::BOOLEAN:
        case TypeId::TINYINT:
            return 1;
        case TypeId::SMALLINT:
            return 2;
        case TypeId::INTEGER:
            return 4;
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::TIMESTAMP:
            return 8;
        case TypeId::VARCHAR:
            return length;
        default:
            assert(false);
    }

}

Column::Column(const char *name, TypeId type)
    : _type(type)
    , _length(TypeSize(type))
{
    assert(type != TypeId::VARCHAR);    // wrong constructor for variable-size type
    ::strncpy(_name, name, MAX_NAME_LENGTH);
}

Column::Column(const char *name, TypeId type, uint32_t length)
    : _type(type)
    , _length(TypeSize(type, length))
{
    assert(type == TypeId::VARCHAR);    // wrong constructor for fixed-size type
    ::strncpy(_name, name, MAX_NAME_LENGTH);
}
