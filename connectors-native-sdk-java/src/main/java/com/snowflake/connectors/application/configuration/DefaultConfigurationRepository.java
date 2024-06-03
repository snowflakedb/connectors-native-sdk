/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration;

import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.snowflake.connectors.common.table.KeyNotFoundException;
import com.snowflake.connectors.common.table.KeyValueTable;
import java.util.Optional;

/**
 * Default implementation of {@link ConfigurationRepository}, which uses the {@link KeyValueTable}
 * for data storage.
 */
public class DefaultConfigurationRepository implements ConfigurationRepository {

  private final KeyValueTable table;

  /**
   * Creates a new {@link DefaultConfigurationRepository}, using the provided table for data
   * storage.
   *
   * @param table key-value table used for data storage
   */
  public DefaultConfigurationRepository(KeyValueTable table) {
    this.table = table;
  }

  @Override
  public <T> Optional<T> fetch(String key, Class<T> clazz) {
    try {
      return Optional.ofNullable(mapVariant(table.fetch(key), clazz));
    } catch (KeyNotFoundException ex) {
      return Optional.empty();
    }
  }

  @Override
  public <T> void update(String key, T value) {
    table.update(key, mapToVariant(value));
  }

  @Override
  public ConfigurationMap fetchAll() {
    var configuration = table.fetchAll();
    return new DefaultConfigurationMap(configuration);
  }

  @Override
  public void delete(String key) {
    table.delete(key);
  }
}
