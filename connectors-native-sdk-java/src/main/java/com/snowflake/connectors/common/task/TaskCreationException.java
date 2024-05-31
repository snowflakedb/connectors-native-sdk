/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when task creation has failed. */
public class TaskCreationException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "FAILED_TO_CREATE_TASK";

  /**
   * Creates a new {@link TaskCreationException}.
   *
   * @param message exception reason message
   */
  public TaskCreationException(String message) {
    super(RESPONSE_CODE, message);
  }
}
