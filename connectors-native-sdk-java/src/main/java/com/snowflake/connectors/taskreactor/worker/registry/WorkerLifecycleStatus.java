/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import com.snowflake.connectors.taskreactor.Dispatcher;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;

/**
 * Lifecycle status for each worker stored in a {@code WORKER_REGISTRY} table.
 *
 * <p>Once a worker becomes {@code ACTIVE}, meaning that all SQL objects are created for it (i.e.
 * queue table + stream + task), it gets a {@link WorkerStatus} assigned.
 */
public enum WorkerLifecycleStatus {

  /** Worker is created and fully operational. */
  ACTIVE,

  /**
   * {@link WorkerOrchestrator} sets this status right after the user increased the number of
   * workers. {@code REQUESTED} workers have no queue table, stream and task created yet.
   */
  REQUESTED,

  /** Set by the {@link Dispatcher} when starting creating worker objects. */
  PROVISIONING,

  /**
   * {@link WorkerOrchestrator} sets this status right after the user decreased the number of
   * workers. {@code UP_FOR_DELETION} workers still have queue table, stream and task existing,
   * however no new work would be assigned to them anymore.
   *
   * <p>If the worker is currently processing, it won't be deleted until it stops.
   */
  UP_FOR_DELETION,

  /** Set by the {@link Dispatcher} when starting deleting worker objects. */
  DELETING,

  /**
   * Worker is deleted. No queue table, stream nor task exist for a worker and it cannot get any
   * work assigned.
   */
  DELETED
}
