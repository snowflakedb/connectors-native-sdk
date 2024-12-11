/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.application.configuration.ConfigurationRepository.CONNECTION_CONFIGURATION_KEY;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link ConnectionConfigurationService}. */
public class DefaultConnectionConfigurationService implements ConnectionConfigurationService {

  private final ConfigurationRepository configurationRepository;

  /**
   * Creates a new {@link DefaultConnectionConfigurationService}.
   *
   * @param configurationRepository {@link ConfigurationRepository} object
   */
  public DefaultConnectionConfigurationService(ConfigurationRepository configurationRepository) {
    this.configurationRepository = configurationRepository;
  }

  @Override
  public void updateConfiguration(Variant configuration) {
    configurationRepository.update(CONNECTION_CONFIGURATION_KEY, configuration);
  }

  @Override
  public Variant getConfiguration() {
    return configurationRepository
        .fetch(CONNECTION_CONFIGURATION_KEY, Variant.class)
        .orElseThrow(ConnectionConfigurationNotFoundException::new);
  }
}
