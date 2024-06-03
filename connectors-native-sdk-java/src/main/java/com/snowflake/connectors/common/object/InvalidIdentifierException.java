/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid identifier is encountered. */
public class InvalidIdentifierException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_IDENTIFIER";

  private static final String MESSAGE = "%s is not a valid Snowflake identifier";

  /**
   * Creates a new {@link InvalidIdentifierException}.
   *
   * @param identifier invalid identifier
   */
  public InvalidIdentifierException(String identifier) {
    super(RESPONSE_CODE, format(MESSAGE, identifier));
  }
}
