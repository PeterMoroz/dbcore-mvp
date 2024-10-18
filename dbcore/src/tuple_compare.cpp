#include <dbcore/tuple_compare.h>
#include <dbcore/value.h>
#include <dbcore/type_id.h>


namespace dbcore
{

Value ToValue(const Schema& schema, const char* data, uint32_t idx)
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

}


using namespace dbcore;


TupleCompare::TupleCompare(const Schema& schema)
    : _schema(schema)
{

}

int TupleCompare::operator()(const char* lhs, const char* rhs) const
{
    const uint32_t col_count = _schema.GetColumnCount();
    for (uint32_t i = 0; i < col_count; i++) {
        const Value lhs_value = ToValue(_schema, lhs, i);
        const Value rhs_value = ToValue(_schema, rhs, i);
        if (lhs_value.CompareLt(rhs_value)) {
            return -1;
        }
        if (lhs_value.CompareGt(rhs_value)) {
            return 1;
        }
    }
    return 0;
}
