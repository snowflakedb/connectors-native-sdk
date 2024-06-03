/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.application.configuration.ConfigurationRepository.CONNECTOR_CONFIGURATION_KEY;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.ALLOWED_PROPERTY_NAMES;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.common.exception.InternalConnectorException;
import com.snowflake.snowpark_java.types.Variant;
import java.io.UncheckedIOException;
import java.util.Set;

/** Default implementation of {@link ConnectorConfigurationService}. */
class DefaultConnectorConfigurationService implements ConnectorConfigurationService {

  private final ConfigurationRepository configurationRepository;

  DefaultConnectorConfigurationService(ConfigurationRepository configurationRepository) {
    this.configurationRepository = configurationRepository;
  }

  @Override
  public void updateConfiguration(Variant configuration) {
    validateFields(configuration);
    configurationRepository.update(CONNECTOR_CONFIGURATION_KEY, configuration);
  }

  @Override
  public void validateFields(Variant configuration) {
    try {
      Set<String> notAllowedFields =
          configuration.asMap().keySet().stream()
              .filter(not(ALLOWED_PROPERTY_NAMES::contains))
              .collect(toSet());
      if (!notAllowedFields.isEmpty()) {
        throw new ConnectorConfigurationParsingException(
            String.format(
                "Fields %s are not allowed in configuration.",
                String.join(", ", notAllowedFields)));
      }
    } catch (UncheckedIOException e) {
      throw new InternalConnectorException("Given configuration is not a valid json");
    }
  }

  @Override
  public Variant getConfiguration() {
    return configurationRepository
        .fetch(CONNECTOR_CONFIGURATION_KEY, Variant.class)
        .orElseThrow(ConnectorConfigurationNotFoundException::new);
  }
}
