/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;

public final class ComponentNames {

  public static final String TASK_REACTOR_SCHEMA = "TASK_REACTOR";
  public static final String QUEUE_TABLE = "QUEUE";
  public static final String QUEUE_STREAM = "QUEUE_STREAM";
  public static final String COMMANDS_QUEUE = "COMMANDS_QUEUE";
  public static final String COMMANDS_QUEUE_STREAM = "COMMANDS_QUEUE_STREAM";
  public static final String WORKER_STATUS_TABLE = "WORKER_STATUS";
  public static final String WORKER_REGISTRY_TABLE = "WORKER_REGISTRY";
  public static final String WORKER_COMBINED_VIEW = "WORKER_QUEUES";
  public static final String CONFIG_TABLE = "CONFIG";
  public static final String DISPATCHER_PROCEDURE = "DISPATCHER";
  public static final Identifier DISPATCHER_TASK = Identifier.from("DISPATCHER_TASK");

  public static Identifier workerQueueTable(WorkerId workerId) {
    return Identifier.fromWithAutoQuoting("WORKER_QUEUE_" + workerId.value());
  }

  public static Identifier workerQueueStream(WorkerId workerId) {
    return Identifier.fromWithAutoQuoting("WORKER_QUEUE_STREAM_" + workerId.value());
  }

  public static Identifier workerTask(WorkerId workerId) {
    return Identifier.fromWithAutoQuoting("WORKER_TASK_" + workerId.value());
  }
}
