/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception;

/** Exception thrown when an internal error has occurred. */
public class InternalConnectorException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INTERNAL_ERROR";

  /**
   * Creates a new {@link InternalConnectorException}.
   *
   * @param message exception message
   */
  public InternalConnectorException(String message) {
    super(RESPONSE_CODE, message);
  }

  public InternalConnectorException(String message, Throwable cause) {
    super(RESPONSE_CODE, message, cause);
  }
}
