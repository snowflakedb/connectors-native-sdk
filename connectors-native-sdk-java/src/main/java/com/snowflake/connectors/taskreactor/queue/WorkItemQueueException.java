/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import com.snowflake.connectors.common.exception.ConnectorException;
import java.sql.SQLException;

/** Exception thrown when statements executed on WorkItemQueue failed . */
public class WorkItemQueueException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "WORK_ITEM_QUEUE_EXCEPTION";

  private static final String MESSAGE = "Failed to execute merge statement on Work Item Queue.";

  /**
   * Creates a new {@link WorkItemQueueException}.
   *
   * @param cause Exception thrown during execution of statement on WorkItemQueue
   */
  public WorkItemQueueException(SQLException cause) {
    super(RESPONSE_CODE, MESSAGE, cause);
  }
}
