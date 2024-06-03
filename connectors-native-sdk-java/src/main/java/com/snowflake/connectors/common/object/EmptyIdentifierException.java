/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an empty identifier is encountered. */
public class EmptyIdentifierException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "EMPTY_IDENTIFIER";

  private static final String MESSAGE = "Provided identifier must not be empty";

  /** Creates a new {@link EmptyIdentifierException}. */
  public EmptyIdentifierException() {
    super(RESPONSE_CODE, MESSAGE);
  }
}
