/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when a non-existent SQL procedure is called. */
public class NonexistentProcedureCallException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "PROCEDURE_NOT_FOUND";

  private static final String MESSAGE = "Nonexistent procedure %s called";

  /**
   * Creates a new {@link NonexistentProcedureCallException}.
   *
   * @param procedure name of the called procedure
   */
  public NonexistentProcedureCallException(String procedure) {
    super(RESPONSE_CODE, String.format(MESSAGE, procedure));
  }
}
