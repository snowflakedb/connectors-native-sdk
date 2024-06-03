/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when scheduler creation failed. */
public class SchedulerCreationException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String ERROR_CODE = "SCHEDULER_CREATION_ERROR";

  /**
   * Creates a new {@link SchedulerCreationException}.
   *
   * @param message exception message
   */
  public SchedulerCreationException(String message) {
    super(ERROR_CODE, message);
  }
}
