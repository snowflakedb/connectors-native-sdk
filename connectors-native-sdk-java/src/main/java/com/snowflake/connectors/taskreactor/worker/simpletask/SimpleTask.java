/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.simpletask;

import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;

/**
 * Interface that should be implemented by users of task reactor and passed over to {@link
 * SimpleTaskWorker} instance.
 *
 * @param <T> Type defining response after executing the task.
 */
@FunctionalInterface
public interface SimpleTask<T> {

  /**
   * Method used for performing simple tasks defined by user.
   *
   * @param workItem work item provided by the dispatcher.
   * @return custom response after executing simple task job.
   */
  T execute(WorkItem workItem);
}
