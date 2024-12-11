/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue;
import com.snowflake.connectors.taskreactor.commands.queue.InMemoryCommandsQueue;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.connectors.taskreactor.dispatcher.InMemoryDispatcherTaskProvider;
import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link TaskReactorInstanceComponentProvider}. */
public class InMemoryTaskReactorInstanceComponentProvider
    implements TaskReactorInstanceComponentProvider {

  private final Map<Identifier, InMemoryCommandsQueue> commandsQueueRepositories;
  private final TaskRepository taskRepository;

  /** Creates a new {@link InMemoryTaskReactorInstanceComponentProvider}. */
  public InMemoryTaskReactorInstanceComponentProvider() {
    this.commandsQueueRepositories = new HashMap<>();
    this.taskRepository = new InMemoryTaskManagement();
  }

  /**
   * Creates a new {@link InMemoryTaskReactorInstanceComponentProvider}.
   *
   * @param commandsQueueRepositories map of command queue repositories for specified Task Reactor
   *     instance identifiers
   * @param taskRepository task repository
   */
  public InMemoryTaskReactorInstanceComponentProvider(
      Map<Identifier, InMemoryCommandsQueue> commandsQueueRepositories,
      TaskRepository taskRepository) {
    this.commandsQueueRepositories = commandsQueueRepositories;
    this.taskRepository = taskRepository;
  }

  @Override
  public CommandsQueue commandsQueue(Identifier instanceSchema) {
    return commandsQueueRepositories.computeIfAbsent(
        instanceSchema, identifier -> new InMemoryCommandsQueue());
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

  /**
   * Returns the commands queue repositories.
   *
   * @return commands queue repositories
   */
  public Map<Identifier, InMemoryCommandsQueue> commandsQueues() {
    return commandsQueueRepositories;
  }
}
