/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import static com.snowflake.connectors.taskreactor.ComponentNames.CONFIG_TABLE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.table.DefaultKeyValueTable;
import com.snowflake.connectors.common.table.KeyValueTable;
import com.snowflake.snowpark_java.Session;

/** Task reactor configuration repository utility. */
public interface ConfigRepository {

  /**
   * Returns a task reactor configuration.
   *
   * @return configuration value
   */
  TaskReactorConfig getConfig();

  /**
   * Updates task reactor configuration.
   *
   * @param key Configuration key to update.
   * @param value Configuration value of key to be updated.
   */
  void update(String key, String value);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultConfigRepository DefaultConfigRepository},
   *       created for the {@code <schema>.CONFIG} table.
   * </ul>
   *
   * @param session Snowpark session object
   * @param instanceName Identifier of Task Reactor schema
   * @return a new repository instance
   */
  static ConfigRepository getInstance(Session session, Identifier instanceName) {
    String tableName = String.format("%s.%s", instanceName.getValue(), CONFIG_TABLE);
    KeyValueTable repository = new DefaultKeyValueTable(session, tableName);

    return new DefaultConfigRepository(repository);
  }
}
