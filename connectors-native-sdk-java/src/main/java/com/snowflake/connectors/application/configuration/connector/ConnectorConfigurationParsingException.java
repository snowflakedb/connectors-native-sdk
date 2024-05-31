/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when provided connector configuration is not valid. */
public class ConnectorConfigurationParsingException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "CONNECTOR_CONFIGURATION_PARSING_ERROR";

  /**
   * Creates a new {@link ConnectorConfigurationParsingException}.
   *
   * @param message exception message
   */
  public ConnectorConfigurationParsingException(String message) {
    super(RESPONSE_CODE, message);
  }
}
