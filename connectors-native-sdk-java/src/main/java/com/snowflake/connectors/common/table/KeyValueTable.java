/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;

/** A representation of a table which holds key-value pairs. */
public interface KeyValueTable {

  /**
   * Fetches the value stored under the provided key.
   *
   * @param key key associated with the value
   * @return value stored under the provided key
   * @throws com.snowflake.connectors.common.table.KeyNotFoundException if no object under provided
   *     key exists
   */
  Variant fetch(String key);

  /**
   * Fetches all key-value pairs and returns them as a Map.
   *
   * @return all key-value pairs
   */
  Map<String, Variant> fetchAll();

  /**
   * Updates the value stored under the provided key.
   *
   * <p>If no value is currently stored under the provided key - it will be inserted into this
   * table.
   *
   * @param key key associated with the value
   * @param value value
   */
  void update(String key, Variant value);

  /**
   * Returns all values from this table where the record matches the provided filter.
   *
   * @param filter filter for the table records
   * @return values from records which match the provided filter
   */
  List<Variant> getAllWhere(Column filter);

  /**
   * Updates the provided field of the Variant value stored under the provided keys.
   *
   * <p>If no value is currently stored under the provided key - it will be ignored.
   *
   * @param keys keys associated with the values
   * @param fieldName name of the updated value field
   * @param fieldValue new value for the updated field
   */
  void updateMany(List<String> keys, String fieldName, Variant fieldValue);

  /**
   * Updates the key-value pairs.
   *
   * <p>If no value is currently stored under the provided key - it will be inserted into this
   * table.
   *
   * @param keyValues list of key-value pairs
   */
  void updateAll(List<KeyValue> keyValues);

  /**
   * Deletes the value stored under the provided key from this table.
   *
   * @param key key associated with the value
   */
  void delete(String key);
}
