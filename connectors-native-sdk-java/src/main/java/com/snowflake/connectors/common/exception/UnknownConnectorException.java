/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception;

/** Exception thrown when an unknown error has occurred. */
public class UnknownConnectorException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "UNKNOWN_ERROR";

  /**
   * Creates a new {@link UnknownConnectorException} with a default message of {@code An unknown
   * error occurred}.
   */
  public UnknownConnectorException() {
    this("An unknown error occurred");
  }

  /**
   * Creates a new {@link UnknownConnectorException} with a provided message.
   *
   * @param message exception message
   */
  public UnknownConnectorException(String message) {
    super(RESPONSE_CODE, message);
  }

  /**
   * Creates a new {@link UnknownConnectorException}, with the provided exception as message source
   * and exception cause.
   *
   * @param exception cause exception
   */
  public UnknownConnectorException(Exception exception) {
    super(RESPONSE_CODE, exception.getMessage(), exception.getCause());
  }
}
