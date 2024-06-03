/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid reference is encountered. */
public class InvalidReferenceException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_REFERENCE";

  private static final String MESSAGE = "%s is not a valid Snowflake reference";

  /**
   * Creates a new {@link InvalidReferenceException}.
   *
   * @param reference invalid reference
   */
  public InvalidReferenceException(String reference) {
    super(RESPONSE_CODE, format(MESSAGE, reference));
  }
}
