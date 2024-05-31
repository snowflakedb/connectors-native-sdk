/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when connection configuration record was not found in the configuration table.
 */
public class ConnectionConfigurationNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "CONNECTION_CONFIGURATION_NOT_FOUND";

  private static final String MESSAGE = "Connection configuration record not found in database.";

  /** Creates a new {@link ConnectionConfigurationNotFoundException}. */
  public ConnectionConfigurationNotFoundException() {
    super(RESPONSE_CODE, MESSAGE);
  }
}
