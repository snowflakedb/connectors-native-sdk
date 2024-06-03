/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.utils;

import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationNotFoundException;
import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationNotFoundException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import java.util.Optional;

/** Utility class for simple connector and connection configuration management. */
public class Configuration {

  private static final String CONNECTOR_CONFIGURATION_KEY = "connector_configuration";
  private static final String CONNECTION_CONFIGURATION_KEY = "connection_configuration";

  private static final String KEY_NOT_FOUND_ERROR = "KEY_NOT_FOUND";
  private static final String KEY_NOT_FOUND_ERROR_MSG =
      "Unable to find [%s] key in the provided configuration.";
  private final Map<String, Variant> config;

  public static Configuration fromCustomConfig(Variant variant) {
    return new Configuration(variant);
  }

  public static Configuration fromConnectorConfig(Session session) {
    var config =
        ConfigurationRepository.getInstance(session)
            .fetch(CONNECTOR_CONFIGURATION_KEY, Variant.class)
            .orElseThrow(ConnectorConfigurationNotFoundException::new);
    return new Configuration(config);
  }

  public static Configuration fromConnectionConfig(Session session) {
    var config =
        ConfigurationRepository.getInstance(session)
            .fetch(CONNECTION_CONFIGURATION_KEY, Variant.class)
            .orElseThrow(ConnectionConfigurationNotFoundException::new);
    return new Configuration(config);
  }

  private Configuration(Variant config) {
    this.config = config.asMap();
  }

  public Optional<String> getValue(String key) {
    return Optional.ofNullable(config.get(key).asString());
  }

  public static ConnectorResponse keyNotFoundResponse(String key) {
    return ConnectorResponse.error(KEY_NOT_FOUND_ERROR, format(KEY_NOT_FOUND_ERROR_MSG, key));
  }
}
