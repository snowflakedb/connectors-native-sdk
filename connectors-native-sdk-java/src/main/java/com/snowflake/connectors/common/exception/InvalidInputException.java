/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception;

/** Exception thrown when an invalid input is encountered. */
public class InvalidInputException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_INPUT";

  /**
   * Creates a new {@link InvalidInputException}.
   *
   * @param message exception message
   */
  public InvalidInputException(String message) {
    super(RESPONSE_CODE, message);
  }
}
