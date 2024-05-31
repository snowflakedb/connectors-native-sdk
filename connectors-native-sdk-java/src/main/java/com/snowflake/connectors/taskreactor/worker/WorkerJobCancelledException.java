/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when new running worker was scheduled for cancellation. */
public class WorkerJobCancelledException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "WORKER_SCHEDULED_FOR_CANCELLATION";

  private static final String MESSAGE = "Running worker was scheduled for cancellation.";

  /** Creates a new {@link WorkerJobCancelledException}. */
  public WorkerJobCancelledException() {
    super(RESPONSE_CODE, MESSAGE);
  }
}
