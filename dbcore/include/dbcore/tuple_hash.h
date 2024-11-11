#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/schema.h>

namespace dbcore
{

class TupleHash final
{
public:
    TupleHash(const Schema& schema, hash_function_ptr_t hash_function);

    /**
     * Calculate hash of the given tuple
     * @param tuple_data the pointer to buffer with tuple data
     * @return combined hash of all of the tuples' fields
    */
    uint32_t operator()(const char* tuple_data) const;

private:
    Schema _schema;
    hash_function_ptr_t _hash_function;  
};

}
