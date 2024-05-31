/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.snowflake;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when not all required privileges are granted to the application instance. */
public class RequiredPrivilegesMissingException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "REQUIRED_PRIVILEGES_MISSING";

  private static final String MESSAGE =
      "To perform this operation the application must be granted the following privileges: %s";

  /**
   * Creates a new {@link RequiredPrivilegesMissingException}.
   *
   * @param requiredPrivileges expected required privileges
   */
  public RequiredPrivilegesMissingException(String... requiredPrivileges) {
    super(RESPONSE_CODE, format(MESSAGE, String.join(", ", requiredPrivileges)));
  }
}
