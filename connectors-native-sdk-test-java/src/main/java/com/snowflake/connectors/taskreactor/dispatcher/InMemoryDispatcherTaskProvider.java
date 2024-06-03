/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.dispatcher;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.TaskRepository;

/** In memory provider of {@link DispatcherTaskManager} instances. */
public class InMemoryDispatcherTaskProvider {

  /**
   * Creates a new {@link DispatcherTaskManager}.
   *
   * @param instanceSchema Task Reactor instance name
   * @param taskRepository task repository
   * @return a new dispatcher task manager instance
   */
  public static DispatcherTaskManager getInstance(
      Identifier instanceSchema, TaskRepository taskRepository) {
    return new DispatcherTaskManager(instanceSchema, taskRepository);
  }
}
