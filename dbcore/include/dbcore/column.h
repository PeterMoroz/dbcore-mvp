#pragma once

#include <dbcore/type_id.h>
#include <dbcore/coretypes.h>

namespace dbcore
{

class Schema;

class Column final
{

friend class Schema;

public:
    static constexpr size_t MAX_NAME_LENGTH = 64;

    // It is not the way by which we should construct Column's instance
    // But default constructor is required by STL-containers
    Column() = default;

private:
    static uint8_t TypeSize(TypeId type, uint32_t length = 0);

public:
    /**
     * Non-variable-length constructor for creating a column.
     * @param name - name of the column
     * @param type - typt of the column
    */
   Column(const char *name, TypeId type);
   
   /**
    * Variable-length constructor for creating a column.
    * @param name - name of the column
    * @param type - type of the column
    * @param length - length of the varlength column
   */
  Column(const char *name, TypeId type, uint32_t length);

  /**
   * @return column length
  */
  uint32_t GetStorageSize() const { return _length; }

  /**
   * @return true if column is inlined, false otherwise
  */
  bool IsInlined() const { return _type != TypeId::VARCHAR; }

  /**
   * @return column's offset in the tuple
  */
  uint32_t GetOffset() const { return _offset; }

  /**
   * @return column type
  */
  TypeId GetType() const { return _type; }

  
private:
    /** Column name */
    char _name[MAX_NAME_LENGTH + 1];
    /** Column value's type */
    TypeId _type{INVALID};
    /** The size of the column */
    uint32_t _length{0};
    /** Column offset in the tuple */
    uint32_t _offset{0};
};

}