/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.response;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid connector response is encountered. */
public class InvalidConnectorResponseException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_RESPONSE";

  private static final String MESSAGE = "Invalid connector response, reason: %s";

  /**
   * Creates a new {@link InvalidConnectorResponseException}.
   *
   * @param reason reason for the response invalidity
   */
  public InvalidConnectorResponseException(String reason) {
    super(RESPONSE_CODE, String.format(MESSAGE, reason));
  }
}
