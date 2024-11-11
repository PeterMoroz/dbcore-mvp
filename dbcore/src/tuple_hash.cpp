#include <dbcore/tuple_hash.h>

#include <cassert>

using namespace dbcore;


TupleHash::TupleHash(const Schema& schema, hash_function_ptr_t hash_function)
    : _schema(schema)
    , _hash_function(hash_function)
{

}

uint32_t TupleHash::operator()(const char* tuple_data) const
{
    // TO DO: the hash calculated on the tuple as whole
    // Might be needed calculate hash on each tuple's field and produce a common hash
    return _hash_function(tuple_data, _schema.GetInlinedStorageSize());
}
