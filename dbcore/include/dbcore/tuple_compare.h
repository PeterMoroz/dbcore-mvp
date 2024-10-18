#pragma once

#include <dbcore/schema.h>

namespace dbcore
{

class Tuple;

/**
 * The class comparator for tuples. 
*/
class TupleCompare final
{

public:
    explicit TupleCompare(const Schema& schema);

    /**
     * Compare two tuples. Both tuples have to be based on the same schema.
     * @param lhs the first tuple's data to compare
     * @param rhs the second tuple's data to compare
     * @return 
    */
    int operator()(const char* lhs, const char* rhs) const;

private:
    Schema _schema;
};

}
