/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** A service for basic management of the connection configuration. */
public interface ConnectionConfigurationService {

  /**
   * Updates the connection configuration record with the provided properties. If the configuration
   * is not present, a new record will be created.
   *
   * @param configuration connection configuration properties
   */
  void updateConfiguration(Variant configuration);

  /**
   * Returns current connection configuration properties.
   *
   * @return connection configuration properties
   * @throws ConnectionConfigurationNotFoundException if connection configuration record does not
   *     currently exist
   */
  Variant getConfiguration();

  /**
   * Returns a new instance of the default service implementation.
   *
   * <p>Default implementation of the service uses:
   *
   * <ul>
   *   <li>a default implementation of {@link ConfigurationRepository}
   *   <li>{@link ConfigurationRepository#CONNECTION_CONFIGURATION_KEY connection_configuration} as
   *       the configuration record key
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new service instance
   */
  static ConnectionConfigurationService getInstance(Session session) {
    var repository = ConfigurationRepository.getInstance(session);
    return new DefaultConnectionConfigurationService(repository);
  }
}
