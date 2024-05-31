/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.snowpark_java.types.Variant;

/** A key-value pair representation. */
public class KeyValue {

  private final String key;
  private final Variant value;

  /**
   * Creates a new {@link KeyValue} with the provided key and value.
   *
   * @param key key
   * @param value value
   */
  public KeyValue(String key, Variant value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Returns the key of this key-value pair.
   *
   * @return key of this key-value pair
   */
  public String key() {
    return key;
  }

  /**
   * Returns the value of this key-value pair.
   *
   * @return value of this key-value pair
   */
  public Variant value() {
    return value;
  }
}
