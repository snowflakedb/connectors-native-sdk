/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.state;

/**
 * An append-only repository for the connector state.
 *
 * @param <T> type of the state object stored in this repository
 */
public interface AppendOnlyStateRepository<T> {

  /**
   * Fetches the most recent state value stored under the provided key in this repository.
   *
   * @param key key associated with the state value
   * @return object stored under the provided key in this repository
   * @throws com.snowflake.connectors.common.table.KeyNotFoundException if no object under provided
   *     key exists in this repository
   */
  T fetch(String key);

  /**
   * Appends the provided state value, with the provided key, to this repository.
   *
   * @param key key associated with the state value
   * @param value state value
   */
  void insert(String key, T value);
}
