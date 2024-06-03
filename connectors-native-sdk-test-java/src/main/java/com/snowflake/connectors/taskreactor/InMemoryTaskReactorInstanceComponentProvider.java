/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.InMemoryTaskRepository;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.commands.queue.InMemoryCommandsQueueRepository;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.connectors.taskreactor.dispatcher.InMemoryDispatcherTaskProvider;
import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link TaskReactorInstanceComponentProvider}. */
public class InMemoryTaskReactorInstanceComponentProvider
    implements TaskReactorInstanceComponentProvider {

  private final Map<Identifier, InMemoryCommandsQueueRepository> commandsQueueRepositories =
      new HashMap<>();
  private final TaskRepository taskRepository = new InMemoryTaskRepository();

  @Override
  public CommandsQueueRepository commandsQueueRepository(Identifier instanceSchema) {
    return commandsQueueRepositories.computeIfAbsent(
        instanceSchema, identifier -> new InMemoryCommandsQueueRepository());
  }

  @Override
  public DispatcherTaskManager dispatcherTaskManager(Identifier instanceSchema) {
    return InMemoryDispatcherTaskProvider.getInstance(instanceSchema, taskRepository);
  }

  /**
   * Returns the task repository.
   *
   * @return task repository
   */
  public TaskRepository taskRepository() {
    return taskRepository;
  }
}
