/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;

/**
 * Exception thrown when performing an operation on a Task Reactor instance which requires the
 * instance to be inactive, but it still is active.
 */
public class TaskReactorInstanceIsActiveException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "TASK_REACTOR_INSTANCE_IS_ACTIVE";

  private static final String MESSAGE =
      "Instance '%s' shouldn't be resumed or started, when updating warehouse.";

  /**
   * Creates a new {@link TaskReactorInstanceIsActiveException}.
   *
   * @param instanceSchema Task Reactor instance name
   */
  public TaskReactorInstanceIsActiveException(String instanceSchema) {
    super(RESPONSE_CODE, format(MESSAGE, instanceSchema));
  }
}
