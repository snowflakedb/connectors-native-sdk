/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/** A representation of a table to which the records can only be appended, never updated. */
public interface AppendOnlyTable {

  /**
   * Fetches the most recent value stored under the provided key.
   *
   * @param key key associated with the value
   * @return most recent value stored under the provided key
   * @throws com.snowflake.connectors.common.table.KeyNotFoundException if no object under provided
   *     key exists
   */
  Variant fetch(String key);

  /**
   * Inserts the provided value under the provided key into this table.
   *
   * @param key key associated with the value
   * @param value value
   */
  void insert(String key, Variant value);

  /**
   * Returns all values from this table where the record matches the provided filter.
   *
   * @param filter filter for the table records
   * @return values from records which match the provided filter
   */
  List<Variant> getAllWhere(Column filter);
}
