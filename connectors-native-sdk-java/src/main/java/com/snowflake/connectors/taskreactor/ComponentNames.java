/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;

/** Helper class containing common Task Reactor object names. */
public final class ComponentNames {

  /** Name of the main Task Reactor schema. */
  public static final String TASK_REACTOR_SCHEMA = "TASK_REACTOR";

  /** Name of the work item queue table in a Task Reactor instance. */
  public static final String QUEUE_TABLE = "QUEUE";

  /** Name of the work item queue stream in a Task Reactor instance. */
  public static final String QUEUE_STREAM = "QUEUE_STREAM";

  /** Name of the command queue table in a Task Reactor instance. */
  public static final String COMMANDS_QUEUE = "COMMANDS_QUEUE";

  /** Name of the command queue stream in a Task Reactor instance. */
  public static final String COMMANDS_QUEUE_STREAM = "COMMANDS_QUEUE_STREAM";

  /** Name of the worker status table in a Task Reactor instance. */
  public static final String WORKER_STATUS_TABLE = "WORKER_STATUS";

  /** Name of the worker registry table in a Task Reactor instance. */
  public static final String WORKER_REGISTRY_TABLE = "WORKER_REGISTRY";

  /** Name of the combined worker queues view in a Task Reactor instance. */
  public static final String WORKER_COMBINED_VIEW = "WORKER_QUEUES";

  /** Name of the config table in a Task Reactor instance. */
  public static final String CONFIG_TABLE = "CONFIG";

  /** Name of the dispatcher procedure in a Task Reactor instance. */
  public static final String DISPATCHER_PROCEDURE = "DISPATCHER";

  /** Name of the dispatcher task in a Task Reactor instance. */
  public static final Identifier DISPATCHER_TASK = Identifier.from("DISPATCHER_TASK");

  /**
   * Creates a name of a queue table for a worker with a given worked id.
   *
   * @param workerId worker id
   * @return name of a worker queue table
   */
  public static Identifier workerQueueTable(WorkerId workerId) {
    return Identifier.from("WORKER_QUEUE_" + workerId.value());
  }

  /**
   * Creates a name of a queue stream for a worker with a given worked id.
   *
   * @param workerId worker id
   * @return name of a worker queue stream
   */
  public static Identifier workerQueueStream(WorkerId workerId) {
    return Identifier.from("WORKER_QUEUE_STREAM_" + workerId.value());
  }

  /**
   * Creates a name of a task for a worker with a given worked id.
   *
   * @param workerId worker id
   * @return name of a worker task
   */
  public static Identifier workerTask(WorkerId workerId) {
    return Identifier.from("WORKER_TASK_" + workerId.value());
  }
}
