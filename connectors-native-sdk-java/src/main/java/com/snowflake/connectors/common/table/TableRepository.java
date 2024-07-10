/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.object.ObjectName;

/** Interface for operations on the table objects. */
public interface TableRepository extends TableLister {
  /**
   * Drops given table if it exists
   *
   * @param table table to drop
   */
  void dropTableIfExists(ObjectName table);
}
