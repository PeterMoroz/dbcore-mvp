#pragma once

#include <dbcore/schema.h>
#include <dbcore/value.h>
#include <dbcore/rid.h>

#include <cstdint>

namespace dbcore
{

using TxTimestamp = int64_t;
static constexpr TxTimestamp INVALID_TX_TS = -1;

struct TupleMeta
{
    /**
     * The transaction timestamp/id of the tuple.
    */
    TxTimestamp _ts{INVALID_TX_TS};
    /**
     * Marks whether the tuple is deleted from table heap.
    */
    bool _is_deleted{false};
};

static constexpr uint32_t TUPLE_META_SIZE = sizeof(TupleMeta);

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
*/
class Tuple final
{
public:
    /** 
     * default constructor (to create a dummy tuple)
     * */ 
    Tuple() = default;

    ~Tuple();

    Tuple(const Tuple &other);
    Tuple& operator=(const Tuple &other);

    Tuple(Tuple &&other);
    Tuple& operator=(Tuple &&other);

    /**
     * constructor to create a tuple based on given values
     * @param value - array values to fill the tuples' fields
     * @param num_values - a number of values
     * @param schema - schema of tuple
    */
   Tuple(const Value values[], size_t num_values, const Schema &schema);

   Tuple(const std::array<Value, MAX_COLUMN_COUNT> &values, uint32_t num_values, const Schema &schema);


   /**
    * constructor to create a tuple based on bytes' representation as it is stored on the table page.
    * @param data - the bytestream with tupledata
    * @param size - the length of data
    * @param rid - the record ID of the constructed tuple
   */
   Tuple(const char* data, uint32_t size, const RID& rid);

   /**
    * Get the tuple's data
   */
   const char* GetData() const { return _data; }

   /**
    * Get length of the tuple
   */
   uint32_t GetLength() const { return _length; }

   /**
    * Generates a key tuple (subset of tuple's fields)
    * @param schema - the tuple's schema
    * @param key_schema - the key's schema
    * @param key_attrs - the array of positions of key's attributes in tuple
    * @param key_attr_count - the number of key attributes
    * @return tuple of key attributes (subset of original tuple)
   */
   Tuple KeyFromTuple(const Schema& schema, const Schema& key_schema,
            std::array<uint32_t, MAX_COLUMN_COUNT> key_attrs, uint32_t key_attr_count) const;


    /**
     * Get value of attribute (field) at specified position using a given scheme and tuple's data.
     * @param schema - schema of tuple
     * @param data - the pointer to tuple's data buffer
     * @param idx - position of field in  the tuple
     * @return value of the field at \ref idx
    */
    static Value GetValue(const Schema& schema, const char* data, uint32_t idx);

private:
    /**
     * Get value of attribute (field) at specified position using a given scheme.
     * @param schema - schema of tuple
     * @param idx - position of field in  the tuple
     * @return value of the field at \ref idx
    */
    Value GetValue(const Schema& schema, uint32_t idx) const;

private:
    RID _rid{};
    char *_data{nullptr};
    uint32_t _length{0};
};

}
