#pragma once

#include <array>

#include <dbcore/column.h>

namespace dbcore
{

class Schema final
{
public:
    /**
     * Construct the schema corresponding the given columns, read left-to-right.
     * @param columns - array of columns that form schema of a table
     * @param column_count - the number of columns in array 
    */
   Schema(Column columns[], uint32_t column_count);

    /**
     * Construct the schema corresponding the given columns, read left-to-right.
     * @param columns - array of columns that form schema of a table
     * @param columns_count - the number of columns in array
    */
   Schema(std::array<Column, MAX_COLUMN_COUNT> columns, uint32_t column_count);

   /**
    * @return true if all columns are inlined, false otherwise
   */
   bool IsInlined() const { return _column_count == _uninlined_count; }

   /**
    * @return the number of bytes used by one tuple
   */
  uint32_t GetInlinedStorageSize() const { return _length; }

  /**
   * @return the number of columns in the schema for the tuple
  */
  uint32_t GetColumnCount() const { return _column_count; }

  /**
   * @return the number of non-inlined columns
  */
  uint32_t GetUninlinedColumnCount() const { return _uninlined_count; }

  /**
   * @return the index of non-inlined column
  */
  uint32_t GetUninlinedColumnIndex(uint32_t pos) const;

  /**
   * Returns a specific column from the schema
   * @param idx index of requested column
   * @return requested column
  */
  const Column& GetColumnAt(uint32_t idx) const { return _columns[idx]; }


  /**
   * Copy schema partially from the given source. 
   * @param src the source Schema object
   * @param attr the array of columns indexes in the source schema
   * @param num_of_attr the number of columns to copy
   * @return Scheme object which is subset of source
  */
  static Schema CopySchema(const Schema& src, uint32_t attr[], uint32_t num_of_attr);


private:
    /** Fixed-length column size, i.e. number of bytes used by one tuple */
    uint32_t _length{0};
    /** All the columns in schema */
    std::array<Column, MAX_COLUMN_COUNT> _columns;
    /** The actual number of columns */
    uint32_t _column_count{0};
    /** Indices of all uninlined columns */
    std::array<uint32_t, MAX_COLUMN_COUNT> _uninlined_columns;
    /** The actual number of uninlined columns */
    uint32_t _uninlined_count{0};

};

}
