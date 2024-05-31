/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration;

import com.snowflake.connectors.common.table.DefaultKeyValueTable;
import com.snowflake.snowpark_java.Session;
import java.util.Optional;

/** A repository for basic storage of the configuration. */
public interface ConfigurationRepository {

  /** Key of connector configuration record. */
  String CONNECTOR_CONFIGURATION_KEY = "connector_configuration";

  /** Key of connection configuration record. */
  String CONNECTION_CONFIGURATION_KEY = "connection_configuration";

  /**
   * Updates the configuration properties in the record stored under the provided key. If the
   * configuration is not present, a new record will be created.
   *
   * @param key key of the configuration record
   * @param value new configuration properties
   * @param <T> type of the provided value
   */
  <T> void update(String key, T value);

  /**
   * Retrieves configuration properties from the record stored under the provided key, and maps them
   * to the provided class.
   *
   * @param key key of the configuration record
   * @param clazz a class to which the retrieved properties will be mapped
   * @param <T> type of the provided class
   * @return an object of a given class, created by mapping the retrieved configuration properties
   */
  <T> Optional<T> fetch(String key, Class<T> clazz);

  /**
   * Retrieves all configuration records and maps them to an {@link ConfigurationMap} instance.
   *
   * @return a configuration map containing all retrieved configuration records
   */
  ConfigurationMap fetchAll();

  /**
   * Deletes the configuration record stored under the provided key. If the record does not exist,
   * then no action is taken.
   *
   * @param key key of the configuration record
   */
  void delete(String key);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses a default implementation of {@link
   * com.snowflake.connectors.common.table.KeyValueTable KeyValueTable}, created for the {@code
   * STATE.APP_CONFIG} table.
   *
   * @param session Snowpark session object
   * @return a new service instance
   */
  static ConfigurationRepository getInstance(Session session) {
    var repository = new DefaultKeyValueTable(session, "STATE.APP_CONFIG");
    return new DefaultConfigurationRepository(repository);
  }
}
