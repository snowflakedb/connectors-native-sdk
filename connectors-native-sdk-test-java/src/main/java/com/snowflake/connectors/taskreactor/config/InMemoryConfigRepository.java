/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link ConfigRepository}. */
public class InMemoryConfigRepository implements ConfigRepository {

  private final Map<String, String> repository = new HashMap<>();

  @Override
  public TaskReactorConfig getConfig() {
    return new TaskReactorConfig(
        repository.get("SCHEMA"),
        repository.get("WORKER_PROCEDURE"),
        repository.get("WORK_SELECTOR_TYPE"),
        repository.get("WORK_SELECTOR"),
        repository.get("EXPIRED_WORK_SELECTOR"),
        repository.get("WAREHOUSE"));
  }

  @Override
  public void update(String key, String value) {
    repository.put(key, value);
  }

  /**
   * Updates the config stored in this repository with the provided values.
   *
   * @param config new config values
   */
  public void updateConfig(Map<String, String> config) {
    config.forEach(this::update);
  }

  /** Clears this repository. */
  public void clear() {
    repository.clear();
  }
}
