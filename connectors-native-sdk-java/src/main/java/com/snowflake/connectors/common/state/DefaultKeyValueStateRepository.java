/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.state;

import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.snowflake.connectors.common.table.KeyValueTable;

/** Default implementation of {@link KeyValueStateRepository}. */
public class DefaultKeyValueStateRepository<T> implements KeyValueStateRepository<T> {

  private final KeyValueTable table;
  private final Class<T> clazz;

  /**
   * Creates a new {@link DefaultKeyValueStateRepository}, using the provided table for data
   * storage.
   *
   * @param table key-value table used for data storage
   * @param clazz class representing state values stored in this repository
   */
  public DefaultKeyValueStateRepository(KeyValueTable table, Class<T> clazz) {
    this.table = table;
    this.clazz = clazz;
  }

  @Override
  public T fetch(String key) {
    return mapVariant(table.fetch(key), clazz);
  }

  @Override
  public void update(String key, T value) {
    table.update(key, mapToVariant(value));
  }
}
