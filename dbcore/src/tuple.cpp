#include <dbcore/tuple.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

using namespace dbcore;

Tuple::Tuple(const Value values[], size_t num_values, const Schema &schema)
{
    assert(num_values == schema.GetColumnCount());

    // 1. Calculate the size of the tuple
    uint32_t tuple_size = schema.GetInlinedStorageSize();
    const uint32_t uninlined_column_count = schema.GetUninlinedColumnCount();
    for (uint32_t i = 0; i < uninlined_column_count; i++) {
        uint32_t idx = schema.GetUninlinedColumnIndex(i);
        const Value &value = values[idx];
        uint32_t len = value.GetStorageSize();
        tuple_size += sizeof(uint32_t) + len;
    }

    // 2. Allocate memory
    _data = static_cast<char *>(::calloc(tuple_size, sizeof(uint8_t)));
    // TO DO: handle case when allocation failed
    _length = tuple_size;

    // 3. Serialize each attribute based on the input value
    uint32_t column_count = schema.GetColumnCount();
    uint32_t offset = schema.GetInlinedStorageSize();

    for (uint32_t i = 0; i < column_count; i++) {
        const Column& column = schema.GetColumnAt(i);
        if (!column.IsInlined()) {
            // write relative offset where the actual varchar data is stored
            *reinterpret_cast<uint32_t *>(_data + column.GetOffset()) = offset;
            // serialize varchar data in-place (size + data)
            values[i].SerializeTo(_data + offset);
            uint32_t len = values[i].GetStorageSize();
            offset += sizeof(uint32_t) + len;
        } else {
            values[i].SerializeTo(_data + column.GetOffset());
        }
    }
}

Tuple::Tuple(const std::array<Value, MAX_COLUMN_COUNT> &values, uint32_t num_values, const Schema &schema)
{
    assert(num_values == schema.GetColumnCount());

    // 1. Calculate the size of the tuple
    uint32_t tuple_size = schema.GetInlinedStorageSize();
    const uint32_t uninlined_column_count = schema.GetUninlinedColumnCount();
    for (uint32_t i = 0; i < uninlined_column_count; i++) {
        uint32_t idx = schema.GetUninlinedColumnIndex(i);
        const Value &value = values[idx];
        uint32_t len = value.GetStorageSize();
        tuple_size += sizeof(uint32_t) + len;
    }

    // 2. Allocate memory
    _data = static_cast<char *>(::calloc(tuple_size, sizeof(uint8_t)));
    // TO DO: handle case when allocation failed
    _length = tuple_size;

    // 3. Serialize each attribute based on the input value
    uint32_t column_count = schema.GetColumnCount();
    uint32_t offset = schema.GetInlinedStorageSize();

    for (uint32_t i = 0; i < column_count; i++) {
        const Column& column = schema.GetColumnAt(i);
        if (!column.IsInlined()) {
            // write relative offset where the actual varchar data is stored
            *reinterpret_cast<uint32_t *>(_data + column.GetOffset()) = offset;
            // serialize varchar data in-place (size + data)
            values[i].SerializeTo(_data + offset);
            uint32_t len = values[i].GetStorageSize();
            offset += sizeof(uint32_t) + len;
        } else {
            values[i].SerializeTo(_data + column.GetOffset());
        }
    }
}

Tuple::Tuple(const char* data, uint32_t size, const RID& rid)
    : _rid(rid)
{
    _data = static_cast<char *>(::malloc(size));
    // TO DO: handle case when malloc failed
    ::memcpy(_data, data, size);
    _length = size;
}


Tuple::~Tuple()
{
    if (_data)
    {
        ::free(_data);
        _data = nullptr;
        _length = 0;
    }
}

Tuple::Tuple(const Tuple &other)
{
    _data = static_cast<char *>(::malloc(other._length));
    // TO DO: handle case when malloc failed
    ::memcpy(_data, other._data, other._length);
    _length = other._length;
    _rid = other._rid;
}

Tuple& Tuple::operator=(const Tuple &other)
{
    if (this != &other)
    {
        char *data = static_cast<char *>(::malloc(other._length));
        // TO DO: handle case when malloc failed
        ::memcpy(data, other._data, other._length);
        std::swap(_data, data);
        _length = other._length;
        _rid = other._rid;
        ::free(data);
    }
    return *this;
}

Tuple::Tuple(Tuple &&other)
{
    std::swap(_data, other._data);
    std::swap(_length, other._length);
    std::swap(_rid, other._rid);
}

Tuple& Tuple::operator=(Tuple &&other)
{
    if (this != &other)
    {
        std::swap(_data, other._data);
        std::swap(_length, other._length);
        std::swap(_rid, other._rid);

        other = Tuple();
    }
    return *this;
}

Tuple Tuple::KeyFromTuple(const Schema& schema, const Schema& key_schema,
            std::array<uint32_t, MAX_COLUMN_COUNT> key_attrs, uint32_t key_attr_count) const
{
    std::array<Value, MAX_COLUMN_COUNT> values;
    for (uint32_t i = 0; i < key_attr_count; i++) {
        values[i] = GetValue(schema, key_attrs[i]);
    }

    return {values, key_attr_count, key_schema};
}

Value Tuple::GetValue(const Schema& schema, const char* data, uint32_t idx)
{
    const auto& column = schema.GetColumnAt(idx);
    const TypeId col_type = column.GetType();
    if (column.IsInlined()) {
        data += column.GetOffset();
    } else {
        const uint32_t offset = *reinterpret_cast<uint32_t *>(const_cast<char *>(data + column.GetOffset()));
        data += offset;
    }

    return Value::DeserializeFrom(data, col_type);
}

Value Tuple::GetValue(const Schema& schema, uint32_t idx) const
{
    return GetValue(schema, GetData(), idx);
}
