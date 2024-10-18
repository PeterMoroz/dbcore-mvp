#include <dbcore/schema.h>

#include <cassert>

using namespace dbcore;

Schema::Schema(Column columns[], uint32_t column_count)
{
    assert(column_count <= MAX_COLUMN_COUNT);
    uint32_t offset = 0;
    uint32_t idx = 0, uninlined_count = 0;
    for (; idx < column_count; idx++) {
        Column& column = columns[idx];
        if (!column.IsInlined()) {
            _uninlined_columns[uninlined_count++] = idx;
        }

        column._offset = offset;
        if (column.IsInlined()) {
            offset += column.GetStorageSize();
        } else {
            offset += sizeof(uint32_t);
        }
        
        _columns[idx] = column;
    }
    _length = offset;
    _column_count = idx;
    _uninlined_count = uninlined_count;
}

Schema::Schema(std::array<Column, MAX_COLUMN_COUNT> columns, uint32_t column_count)
{
    assert(column_count <= MAX_COLUMN_COUNT);
    uint32_t offset = 0;
    uint32_t idx = 0, uninlined_count = 0;
    for (; idx < column_count; idx++) {
        Column& column = columns[idx];
        if (!column.IsInlined()) {
            _uninlined_columns[uninlined_count++] = idx;
        }

        column._offset = offset;
        if (column.IsInlined()) {
            offset += column.GetStorageSize();
        } else {
            offset += sizeof(uint32_t);
        }
        
        _columns[idx] = column;
    }
    _length = offset;
    _column_count = idx;
    _uninlined_count = uninlined_count;
}

uint32_t Schema::GetUninlinedColumnIndex(uint32_t pos) const
{
    assert(pos < MAX_COLUMN_COUNT && pos < _uninlined_count);
    return _uninlined_columns[pos];
}

Schema Schema::CopySchema(const Schema& src, uint32_t attr[], uint32_t num_of_attr)
{
    std::array<Column, MAX_COLUMN_COUNT> columns;

    for (uint32_t i = 0; i < num_of_attr; i++) {
        const uint32_t pos = attr[i];
        assert(pos < src._column_count);
        columns[i] = src._columns[pos];
    }
    return Schema{columns, num_of_attr};
}
