/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when an invalid schema name is encountered. */
public class InvalidSchemaNameException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_SCHEMA_NAME";

  private static final String MESSAGE = "'%s' is not a valid Snowflake schema name";

  /**
   * Creates a new {@link InvalidSchemaNameException}.
   *
   * @param schemaName invalid schema name
   */
  public InvalidSchemaNameException(String schemaName) {
    super(RESPONSE_CODE, format(MESSAGE, schemaName));
  }
}
