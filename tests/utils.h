#pragma once

#include <dbcore/tuple.h>

namespace testutils
{

/**
 * Use a fixed schema to construct a random tuple.
*/
dbcore::Tuple ConstructTuple(const dbcore::Schema &schema);

}