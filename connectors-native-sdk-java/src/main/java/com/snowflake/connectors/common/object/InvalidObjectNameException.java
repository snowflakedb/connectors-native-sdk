/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid object name is encountered. */
public class InvalidObjectNameException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_OBJECT_NAME";

  private static final String MESSAGE = "'%s' is not a valid Snowflake object name";

  /**
   * Creates a new {@link InvalidObjectNameException}.
   *
   * @param objectName invalid object name
   */
  public InvalidObjectNameException(String objectName) {
    super(RESPONSE_CODE, format(MESSAGE, objectName));
  }
}
