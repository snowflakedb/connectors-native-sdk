/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an unknown SQL error has occurred. */
public class UnknownSqlException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "UNKNOWN_SQL_ERROR";

  /**
   * Creates a new {@link UnknownSqlException} with a default message of {@code An unknown SQL error
   * occurred}.
   */
  public UnknownSqlException() {
    super(RESPONSE_CODE, "An unknown SQL error occurred");
  }

  /**
   * Creates a new {@link UnknownSqlException} with a provided message and called procedure
   * information.
   *
   * @param procedure name of the procedure called when the exception occurred
   * @param message exception message
   */
  public UnknownSqlException(String procedure, String message) {
    super(
        RESPONSE_CODE,
        String.format("Unknown error occurred when calling %s procedure: %s", procedure, message));
  }
}
