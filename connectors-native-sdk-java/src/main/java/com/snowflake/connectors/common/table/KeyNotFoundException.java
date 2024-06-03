/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when no object under provided key exists. */
public class KeyNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "KEY_NOT_FOUND";

  private static final String MESSAGE = "Key not found in configuration: %s";

  /**
   * Creates a new {@link KeyNotFoundException}.
   *
   * @param key key not found
   */
  public KeyNotFoundException(String key) {
    super(RESPONSE_CODE, String.format(MESSAGE, key));
  }
}
