/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;

/** InMemory implementation of {@link ConnectorConfigurationService}. */
public class InMemoryConnectorConfigurationService extends DefaultConnectorConfigurationService {

  /**
   * Creates a new {@link InMemoryConnectorConfigurationService}.
   *
   * @param configurationRepository configuration repository
   */
  public InMemoryConnectorConfigurationService(ConfigurationRepository configurationRepository) {
    super(configurationRepository);
  }
}
