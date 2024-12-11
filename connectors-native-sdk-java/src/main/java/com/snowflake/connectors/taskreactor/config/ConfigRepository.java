/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.config;

import static com.snowflake.connectors.taskreactor.ComponentNames.CONFIG_TABLE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
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
    ObjectName configTableName = ObjectName.from(instanceName, Identifier.from(CONFIG_TABLE));
    return new DefaultConfigRepository(session, configTableName);
  }
}
