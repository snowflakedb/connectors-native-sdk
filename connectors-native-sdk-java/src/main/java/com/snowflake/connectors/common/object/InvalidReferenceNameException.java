/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid reference name is encountered. */
public class InvalidReferenceNameException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_REFERENCE_NAME";

  private static final String MESSAGE = "'%s' is not a valid name for a Snowflake reference";

  /**
   * Creates a new {@link InvalidReferenceNameException}.
   *
   * @param referenceName invalid reference name
   */
  public InvalidReferenceNameException(String referenceName) {
    super(RESPONSE_CODE, format(MESSAGE, referenceName));
  }
}
