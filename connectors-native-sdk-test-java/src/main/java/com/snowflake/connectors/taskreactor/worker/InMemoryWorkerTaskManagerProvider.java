/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.config.InMemoryConfigRepository;

/** In memory provider of {@link WorkerTaskManager} instances. */
public class InMemoryWorkerTaskManagerProvider {

  /**
   * Creates a new in memory instance of {@link WorkerTaskManager}.
   *
   * @param instanceSchema Task Reactor instance name
   * @param taskRepository task repository
   * @return a new worker task manager instance
   */
  public static WorkerTaskManager getInstance(
      Identifier instanceSchema, TaskRepository taskRepository) {
    return new WorkerTaskManager(instanceSchema, new InMemoryConfigRepository(), taskRepository);
  }
}
