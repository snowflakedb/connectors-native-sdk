/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.state;

/**
 * A key-value repository for the connector state.
 *
 * @param <T> type of the state object stored in this repository
 */
public interface KeyValueStateRepository<T> {

  /**
   * Fetches the state value stored under the provided key in this repository.
   *
   * @param key key associated with the state value
   * @return object stored under the provided key in this repository
   * @throws com.snowflake.connectors.common.table.KeyNotFoundException if no object under provided
   *     key exists in this repository
   */
  T fetch(String key);

  /**
   * Updates the provided state value, stored under the provided key, in this repository.
   *
   * <p>If no value is currently stored under the provided key - it will be inserted into this
   * repository.
   *
   * @param key key associated with the state value
   * @param value state value
   */
  void update(String key, T value);
}
