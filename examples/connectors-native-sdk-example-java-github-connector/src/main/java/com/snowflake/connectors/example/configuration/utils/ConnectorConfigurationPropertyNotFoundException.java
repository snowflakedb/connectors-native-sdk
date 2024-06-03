/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.utils;

/**
 * Exception thrown when the provided configuration property does not exist in the connector
 * configuration table.
 */
public class ConnectorConfigurationPropertyNotFoundException extends RuntimeException {

  private static final String ERROR_MESSAGE =
      "Property [%s] not found in the Connector Configuration";

  public ConnectorConfigurationPropertyNotFoundException(String property) {
    super(String.format(ERROR_MESSAGE, property));
  }
}
