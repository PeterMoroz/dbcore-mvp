#include <dbcore/value.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include <algorithm>
#include <limits>

using namespace dbcore;


static constexpr uint32_t INVALID_SIZE = std::numeric_limits<uint32_t>::max();

static constexpr uint32_t VARCHAR_MAX_LENGTH = std::numeric_limits<uint32_t>::max();

Value::~Value()
{
    if (_type_id == TypeId::VARCHAR && _manage_data)
    {
        ::free(_value._varlen);
        _value._varlen = nullptr;
        _manage_data = false;
    }
}

Value::Value(const Value &other)
    : _type_id(other._type_id)
    , _size(other._size)
    , _manage_data(other._manage_data)
    , _value(other._value)
{
    if (_type_id == TypeId::VARCHAR) 
    {
        if (_size == INVALID_SIZE)
            _value._varlen = nullptr;
        else {
            if (_manage_data) {
                _value._varlen = (char *)::malloc(_size);
                ::memcpy(_value._varlen, other._value._varlen, _size);
            }
        }
    }
}

Value& Value::operator=(const Value &other)
{
    if (this != &other)
    {
        if (other._manage_data)
        {
            char *varlen = (char *)::malloc(other._size);
            // TO DO: handle case when allocation failed
            ::memcpy(varlen, other._value._varlen, other._size);
            if (_type_id == TypeId::VARCHAR && _manage_data)
            {
                ::free(_value._varlen);
                _value._varlen = varlen;
            }
            _value._varlen = varlen;
        }
        else
        {
            if (_type_id == TypeId::VARCHAR && _manage_data)
            {
                ::free(_value._varlen);
                _value._varlen = nullptr;
            }
            _value = other._value;
        }
        _type_id = other._type_id;
        _size = other._size;
        _manage_data = other._manage_data;
    }
    return *this;
}

Value::Value(TypeId type)
    : _manage_data(false)
    , _type_id(type)
    , _size(INVALID_SIZE)
{
}

// BOOLEAN and TINYINT
Value::Value(TypeId type, int8_t v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::BOOLEAN:
            _value._boolean = v;
            break;

        case TypeId::TINYINT:
            _value._tinyint = v;
            break;

        case TypeId::SMALLINT:
            _value._smallint = v;
            break;

        case TypeId::INTEGER:
            _value._integer = v;
            break;

        case TypeId::BIGINT:
            _value._bigint = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for 1-byte integer value
    }
}

// DECIMAL
Value::Value(TypeId type, double v)
{
    switch (type)
    {
        case TypeId::DECIMAL:
            _value._decimal = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for floating-point double-precision value
    }
}

Value::Value(TypeId type, float v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::DECIMAL:
            _value._decimal = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for floating-point value
    }
}

// SMALLINT
Value::Value(TypeId type, int16_t v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::BOOLEAN:
            _value._boolean = v;
            break;

        case TypeId::TINYINT:
            _value._tinyint = v;
            break;

        case TypeId::SMALLINT:
            _value._smallint = v;
            break;

        case TypeId::INTEGER:
            _value._integer = v;
            break;

        case TypeId::BIGINT:
            _value._bigint = v;
            break;

        case TypeId::TIMESTAMP:
            _value._timestamp = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for 2-byte integer value
    }
}

// INTEGER
Value::Value(TypeId type, int32_t v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::BOOLEAN:
            _value._boolean = v;
            break;

        case TypeId::TINYINT:
            _value._tinyint = v;
            break;

        case TypeId::SMALLINT:
            _value._smallint = v;
            break;

        case TypeId::INTEGER:
            _value._integer = v;
            break;

        case TypeId::BIGINT:
            _value._bigint = v;
            break;

        case TypeId::TIMESTAMP:
            _value._timestamp = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for 4-byte integer value
    }
}

// BIGINT
Value::Value(TypeId type, int64_t v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::BOOLEAN:
            _value._boolean = v;
            break;

        case TypeId::TINYINT:
            _value._tinyint = v;
            break;

        case TypeId::SMALLINT:
            _value._smallint = v;
            break;

        case TypeId::INTEGER:
            _value._integer = v;
            break;

        case TypeId::BIGINT:
            _value._bigint = v;
            break;

        case TypeId::TIMESTAMP:
            _value._timestamp = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for 8-byte integer value
    }
}

// TIMESTAMP
Value::Value(TypeId type, uint64_t v)
    : Value(type)
{
    switch (type)
    {
        case TypeId::BIGINT:
            _value._bigint = v;
            break;

        case TypeId::TIMESTAMP:
            _value._timestamp = v;
            break;

        default:
            assert(false);  // incompatible type, invalid type for 8-byte unsigned integer value
    }
}

// VARCHAR
Value::Value(TypeId type, const char *data, uint32_t len, bool manage_data)
    : Value(type)
{
    switch (type)
    {
        case TypeId::VARCHAR:
            if (data == nullptr) {
                _value._varlen = nullptr;
                _size = INVALID_SIZE;
            } else {
                _manage_data = manage_data;
                if (_manage_data) {
                    assert(len < VARCHAR_MAX_LENGTH);
                    _value._varlen = (char *)::malloc(len);
                    // TO DO: handle case when allocation failed
                    ::memcpy(_value._varlen, data, len);
                } else {
                    _value._const_varlen = data;
                }
                _size = len;                
            }
            break;
        default:
            assert(false);  // incompatible type, invalid type for variable length value
    }
}

uint32_t Value::GetStorageSize() const
{
    return _size == INVALID_SIZE ? 0 : _size;
}

bool Value::IsNull() const
{
    return _size == INVALID_SIZE;
}

void Value::SerializeTo(char *storage) const
{
    switch (_type_id)
    {
        case TypeId::BOOLEAN:
            *reinterpret_cast<int8_t *>(storage) = _value._boolean;
            return;
        case TypeId::TINYINT:
            *reinterpret_cast<int8_t *>(storage) = _value._tinyint;
            return;
        case TypeId::SMALLINT:
            *reinterpret_cast<int16_t *>(storage) = _value._smallint;
            return;
        case TypeId::INTEGER:
            *reinterpret_cast<int32_t *>(storage) = _value._integer;
            return;
        case TypeId::BIGINT:
            *reinterpret_cast<int64_t *>(storage) = _value._bigint;
            return;
        case TypeId::DECIMAL:
            *reinterpret_cast<int64_t *>(storage) = _value._decimal;
            return;
        case TypeId::VARCHAR:
            {
                ::memcpy(storage, &_size, sizeof(uint32_t));
                if (_size != INVALID_SIZE)
                    ::memcpy(storage + sizeof(uint32_t), _value._varlen, _size);
            }
            break;
        case TypeId::TIMESTAMP:
            *reinterpret_cast<uint64_t *>(storage) = _value._timestamp;
            return;
        default:
            assert(false);
            return;
    }

}

Value Value::DeserializeFrom(const char* storage, TypeId type_id)
{
    switch (type_id)
    {
        case TypeId::BOOLEAN: {
            int8_t v = *reinterpret_cast<const int8_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::TINYINT: {
            int8_t v = *reinterpret_cast<const int8_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::SMALLINT: {
            int16_t v = *reinterpret_cast<const int16_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::INTEGER: {
            int32_t v = *reinterpret_cast<const int32_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::BIGINT: {
            int64_t v = *reinterpret_cast<const int64_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::DECIMAL: {
            int64_t v = *reinterpret_cast<const int64_t *>(storage);
            return Value{type_id, v};
        }
        case TypeId::VARCHAR: {
            const uint32_t len = *reinterpret_cast<const uint32_t *>(storage);
            return Value{type_id, storage + sizeof(uint32_t), len, false};
        }
        case TypeId::TIMESTAMP: {
            uint64_t v = *reinterpret_cast<const uint64_t *>(storage);
            return Value{type_id, v};            
        }
        default:
            assert(false);
            return Value{};
    }
}

bool Value::CompareLt(const Value& other) const
{
    assert(_type_id == other._type_id);
    switch (_type_id)
    {
        case TypeId::BOOLEAN:
            return (!_value._boolean && other._value._boolean);
        case TypeId::TINYINT:
            return _value._tinyint < other._value._tinyint;
        case TypeId::SMALLINT:
            return _value._smallint < other._value._smallint;
        case TypeId::INTEGER:
            return _value._integer < other._value._integer;
        case TypeId::BIGINT:
            return _value._bigint < other._value._bigint;
        case TypeId::DECIMAL:
            return _value._decimal < other._value._decimal;
        case TypeId::VARCHAR:
            {
                assert(_value._varlen == other._value._varlen);
                const uint32_t cmplen = std::min(_size, other._size);
                return (::strncmp(_value._varlen, other._value._varlen, cmplen) < 0);
            }
            break;
        case TypeId::TIMESTAMP:
            return _value._timestamp < other._value._timestamp;
        default:
            assert(false);
            return false;
    }
    return false;
}

bool Value::CompareGt(const Value& other) const
{
    assert(_type_id == other._type_id);
    switch (_type_id)
    {
        case TypeId::BOOLEAN:
            return (_value._boolean && !other._value._boolean);
        case TypeId::TINYINT:
            return _value._tinyint > other._value._tinyint;
        case TypeId::SMALLINT:
            return _value._smallint > other._value._smallint;
        case TypeId::INTEGER:
            return _value._integer > other._value._integer;
        case TypeId::BIGINT:
            return _value._bigint > other._value._bigint;
        case TypeId::DECIMAL:
            return _value._decimal > other._value._decimal;
        case TypeId::VARCHAR:
            {
                assert(_value._varlen == other._value._varlen);
                const uint32_t cmplen = std::min(_size, other._size);
                return (::strncmp(_value._varlen, other._value._varlen, cmplen) > 0);
            }
            break;
        case TypeId::TIMESTAMP:
            return _value._timestamp > other._value._timestamp;
        default:
            assert(false);
            return false;
    }
    return false;
}
