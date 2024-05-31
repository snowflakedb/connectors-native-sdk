/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when connector configuration record was not found in the configuration table.
 */
public class ConnectorConfigurationNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "CONNECTOR_CONFIGURATION_NOT_FOUND";

  private static final String MESSAGE = "Connector configuration record not found in database.";

  /** Creates a new {@link ConnectorConfigurationNotFoundException}. */
  public ConnectorConfigurationNotFoundException() {
    super(RESPONSE_CODE, MESSAGE);
  }
}
