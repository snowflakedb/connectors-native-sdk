/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import com.snowflake.connectors.taskreactor.Dispatcher;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus;

/**
 * Status describing {@link WorkerLifecycleStatus#ACTIVE} workers. This status determines whether a
 * worker is considered as available for dispatcher to assign work to or not.
 *
 * <p>Those statuses are stored in {@code WORKER_STATUS} table. This table is append-only to prevent
 * from excessive table locking when the status is updated by multiple workers at the same time.
 * Status for a worker is computed as the newest row for given {@code WORKER_ID} in the table.
 */
public enum WorkerStatus {

  /**
   * Worker is operational and work could be assigned to it.
   *
   * <p>This status is assigned to worker right after creation of the worker, or when the {@link
   * Worker} finishes the execution.
   */
  AVAILABLE,

  /** Worker gets this status assigned once the {@link Dispatcher} assigns work to it. */
  WORK_ASSIGNED,

  /** {@link Worker} class sets this status on the beginning of execution. */
  IN_PROGRESS,

  /**
   * If a new row with {@code cancelOngoingExecution: true} flag is added to the input queue, worker
   * gets this status assigned by the {@link Dispatcher}. Then the {@link Worker}, in each
   * iteration, checks if it needs to stop its execution.
   */
  SCHEDULED_FOR_CANCELLATION
}
