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

private:
    RID _rid{};
    char *_data{nullptr};
    uint32_t _length{0};
};

}
