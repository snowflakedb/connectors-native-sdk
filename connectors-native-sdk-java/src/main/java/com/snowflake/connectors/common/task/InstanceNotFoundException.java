/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when a nonexistent Task Reactor instance name was provided. */
public class InstanceNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INSTANCE_NOT_FOUND";

  private static final String MESSAGE = "Provided '%s' instance does not exist.";

  /**
   * Creates a new {@link InstanceNotFoundException}.
   *
   * @param instance nonexistent Task Reactor instance name
   */
  public InstanceNotFoundException(String instance) {
    super(RESPONSE_CODE, String.format(MESSAGE, instance));
  }
}
