#include "utils.h"

#include <chrono>
#include <random>
#include <limits>

#include <dbcore/value.h>

namespace testutils
{

dbcore::Tuple ConstructTuple(const dbcore::Schema &schema)
{
    using namespace dbcore;
    std::array<Value, MAX_COLUMN_COUNT> values;
    uint32_t num_values = 0;

    Value v;

    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 rnd_generator(seed);

    const uint32_t column_count = schema.GetColumnCount();
    for (uint32_t i = 0; i < column_count; i++) {
        const Column& column = schema.GetColumnAt(i);
        const TypeId type = column.GetType();
        const auto rnd_value = rnd_generator();
        switch (type)
        {
            case TypeId::BOOLEAN:
                v = Value(type, static_cast<int8_t>(rnd_value % 2));
                break;

            case TypeId::TINYINT:
                v = Value(type, static_cast<int8_t>(rnd_value % std::numeric_limits<int8_t>::max()));
                break;

            case TypeId::SMALLINT:
                v = Value(type, static_cast<int16_t>(rnd_value % std::numeric_limits<int16_t>::max()));
                break;

            case TypeId::INTEGER:
                v = Value(type, static_cast<int32_t>(rnd_value % std::numeric_limits<int32_t>::max()));
                break;

            case TypeId::BIGINT:
                v = Value(type, static_cast<int64_t>(rnd_value % std::numeric_limits<int64_t>::max()));
                break;

            case TypeId::VARCHAR:
                {
                    static const char alphanum[] = "0123456789"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                        "abcdefghijklmnopqrstuvwxyz";
                    const uint32_t len = static_cast<uint32_t>(rnd_value % 9 + 1);
                    char str[10] = { '\0' };
                    for (uint32_t i = 0; i < len; i++) {
                        str[i] = alphanum[rnd_generator() % (sizeof(alphanum) - 1)];
                    }
                    str[len] = '\0';
                    v = Value(type, str, len + 1, true);
                }
                break;
        }
        values[i] = v;
    }

    return {values, column_count, schema};
}

}

