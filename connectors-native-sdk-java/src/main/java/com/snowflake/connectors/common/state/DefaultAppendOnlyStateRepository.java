/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.state;

import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.snowflake.connectors.common.table.AppendOnlyTable;

/** Default implementation of {@link AppendOnlyStateRepository}. */
public class DefaultAppendOnlyStateRepository<T> implements AppendOnlyStateRepository<T> {

  private final AppendOnlyTable table;
  private final Class<T> clazz;

  /**
   * Creates a new {@link DefaultAppendOnlyStateRepository}, using the provided table for data
   * storage.
   *
   * @param table append-only table used for data storage
   * @param clazz class representing state values stored in this repository
   */
  public DefaultAppendOnlyStateRepository(AppendOnlyTable table, Class<T> clazz) {
    this.table = table;
    this.clazz = clazz;
  }

  @Override
  public T fetch(String key) {
    return mapVariant(table.fetch(key), clazz);
  }

  @Override
  public void insert(String key, T value) {
    table.insert(key, mapToVariant(value));
  }
}
