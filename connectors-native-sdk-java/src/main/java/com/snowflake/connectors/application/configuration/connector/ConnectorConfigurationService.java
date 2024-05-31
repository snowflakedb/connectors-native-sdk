/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** A service for basic management of the connector configuration. */
public interface ConnectorConfigurationService {

  /**
   * Updates the connector configuration record with the provided properties. If the configuration
   * is not present, a new record will be created.
   *
   * @param configuration connector configuration properties, which keys must be allowed by the
   *     {@link ConnectorConfigurationKey} values
   * @throws ConnectorConfigurationParsingException if a key of a provided property is not allowed
   *     by the {@link ConnectorConfigurationKey} values
   */
  void updateConfiguration(Variant configuration);

  /**
   * Validates if all the provided connector configuration properties are allowed by the {@link
   * ConnectorConfigurationKey} values.
   *
   * @param configuration connector configuration properties, which keys must be allowed by the
   *     {@link ConnectorConfigurationKey} values
   * @throws ConnectorConfigurationParsingException if a key of a provided property is not allowed
   *     by the {@link ConnectorConfigurationKey} values
   */
  void validateFields(Variant configuration);

  /**
   * Retrieves current connector configuration properties.
   *
   * @return connector configuration properties
   * @throws ConnectorConfigurationNotFoundException if connector configuration record does not
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
   *   <li>{@link ConfigurationRepository#CONNECTOR_CONFIGURATION_KEY connector_configuration} as
   *       the configuration record key
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new service instance
   */
  static ConnectorConfigurationService getInstance(Session session) {
    var repository = ConfigurationRepository.getInstance(session);
    return new DefaultConnectorConfigurationService(repository);
  }
}
