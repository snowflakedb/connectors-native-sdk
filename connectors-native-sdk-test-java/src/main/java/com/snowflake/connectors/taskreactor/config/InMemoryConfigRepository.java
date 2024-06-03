/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.common.table.KeyValue;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** In memory implementation of {@link ConfigRepository}. */
public class InMemoryConfigRepository implements ConfigRepository {

  private final ConfigRepository configRepository;
  private final InMemoryDefaultKeyValueTable store;

  public InMemoryConfigRepository() {
    this.store = new InMemoryDefaultKeyValueTable();
    this.configRepository = new DefaultConfigRepository(store);
  }

  @Override
  public TaskReactorConfig getConfig() {
    return configRepository.getConfig();
  }

  @Override
  public void update(String key, String value) {
    store.update(key, new Variant(value));
  }

  /**
   * Updates the config stored in this repository with the provided values.
   *
   * @param config new config values
   */
  public void updateConfig(Map<String, String> config) {
    List<KeyValue> keyValues =
        config.entrySet().stream()
            .map(entry -> new KeyValue(entry.getKey(), new Variant(entry.getValue())))
            .collect(Collectors.toList());
    store.updateAll(keyValues);
  }

  /** Clears this repository. */
  public void clear() {
    store.getRepository().clear();
  }
}
