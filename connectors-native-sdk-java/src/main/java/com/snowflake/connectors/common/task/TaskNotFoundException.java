/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.object.ObjectName;

/** Exception thrown when a specified task was not found. */
public class TaskNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "TASK_NOT_FOUND";

  private static final String MESSAGE = "Provided %s task does not exist.";

  /**
   * Creates a new {@link TaskNotFoundException}.
   *
   * @param taskName task object name
   */
  public TaskNotFoundException(ObjectName taskName) {
    super(RESPONSE_CODE, String.format(MESSAGE, taskName.getValue()));
  }
}
