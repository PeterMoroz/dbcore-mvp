#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/type_id.h>

namespace dbcore
{

class Value final
{
public:

    Value() : Value(TypeId::INVALID) {}

    ~Value();

    Value(const Value &other);
    Value& operator=(const Value &other);

    explicit Value(TypeId type);

    // BOOLEAN and TINYINT
    Value(TypeId type, int8_t v);

    // DECIMAL
    Value(TypeId type, double v);
    Value(TypeId type, float v);

    // SMALLINT
    Value(TypeId type, int16_t v);

    // INTEGER
    Value(TypeId type, int32_t v);

    // BIGINT
    Value(TypeId type, int64_t v);

    // TIMESTAMP
    Value(TypeId type, uint64_t v);

    // VARCHAR
    Value(TypeId type, const char *data, uint32_t len, bool manage_data);


    /**
     * Get the length of variable length data.
    */
    uint32_t GetStorageSize() const;

    /**
    * Test if the NULL value is hold;
    */
    bool IsNull() const;

    /**
     * Serialize the value into the given location.
     */
    void SerializeTo(char *storage) const;

    /**
     * Deserialize a value of the given type from the given storage space
     * @warning in case of varlen type, the returned object doesn't manage data 
     * (i.e. doesn't get ownership)
    */
    static Value DeserializeFrom(const char* storage, TypeId type_id);

    bool CompareLt(const Value& other) const;
    bool CompareGt(const Value& other) const;

private:
    union Val {
        int8_t _boolean;
        int8_t _tinyint;
        int16_t _smallint;
        int32_t _integer;
        int64_t _bigint;
        double _decimal;
        uint64_t _timestamp;
        char *_varlen;
        const char *_const_varlen;
    } _value;

    uint32_t _size;

    bool _manage_data;
    TypeId _type_id;
};

}