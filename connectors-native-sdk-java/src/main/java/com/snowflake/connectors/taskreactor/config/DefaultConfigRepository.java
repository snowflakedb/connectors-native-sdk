/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import com.snowflake.connectors.common.table.KeyValueTable;
import com.snowflake.connectors.util.variant.VariantMapper;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;

/** Default implementation of {@link ConfigRepository} repository. */
class DefaultConfigRepository implements ConfigRepository {

  private final KeyValueTable repository;

  DefaultConfigRepository(KeyValueTable keyValueTable) {
    this.repository = keyValueTable;
  }

  @Override
  public TaskReactorConfig getConfig() {
    Map<String, Variant> configuration = repository.fetchAll();
    return VariantMapper.mapVariant(new Variant(configuration), TaskReactorConfig.class);
  }

  @Override
  public void update(String key, String value) {
    repository.update(key, new Variant(value));
  }
}
