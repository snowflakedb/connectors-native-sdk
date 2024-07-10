/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration;

import java.util.Optional;

/** A collection of key-value pairs, with an additional mapping for values. */
public interface ConfigurationMap {

  /**
   * Returns the value stored under the provided key and maps it to the provided class. If the key
   * is not found in the map - an empty Optional is returned.
   *
   * @param key key under which the value is stored
   * @param clazz a class to which the value will be mapped
   * @param <T> type of the provided class
   * @return an object of a given class, created by mapping the value
   */
  <T> Optional<T> get(String key, Class<T> clazz);

  /**
   * Returns the number of existing keys in the configuration
   *
   * @return the number of existing keys
   */
  int size();
}
