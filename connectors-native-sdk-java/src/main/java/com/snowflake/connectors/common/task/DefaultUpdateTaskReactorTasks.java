/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.UPDATE_WAREHOUSE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.registry.TaskReactorInstance;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;

/** Default implementation of {@link UpdateTaskReactorTasks}. */
class DefaultUpdateTaskReactorTasks implements UpdateTaskReactorTasks {

  private final InstanceRegistryRepository instanceRegistryRepository;
  private final TaskReactorInstanceComponentProvider componentProvider;
  private final TaskRepository taskRepository;

  DefaultUpdateTaskReactorTasks(
      InstanceRegistryRepository instanceRegistryRepository,
      TaskReactorInstanceComponentProvider componentProvider,
      TaskRepository taskRepository) {
    this.instanceRegistryRepository = instanceRegistryRepository;
    this.componentProvider = componentProvider;
    this.taskRepository = taskRepository;
  }

  @Override
  public void update(Identifier warehouse) {
    instanceRegistryRepository.fetchAll().stream()
        .map(TaskReactorInstance::instanceName)
        .map(Identifier::getName)
        .forEach(instance -> updateInstance(instance, warehouse));
  }

  @Override
  public void updateInstance(String instance, Identifier warehouse) {
    validateInstanceIsNotActive(instance);

    var instanceIdentifier = Identifier.fromWithAutoQuoting(instance);
    var commandQueueRepository = componentProvider.commandsQueueRepository(instanceIdentifier);

    var dispatcherTask = ObjectName.from(instance, "DISPATCHER_TASK");
    var schedulerTaskRef = taskRepository.fetch(dispatcherTask);

    updateWarehouse(warehouse, commandQueueRepository, schedulerTaskRef);
  }

  /**
   * Updates dispatcher task warehouse and adds update warehouse command to the queue.
   *
   * @param warehouseName warehouse name
   * @param commandsQueueRepository instance of {@link CommandsQueueRepository}
   * @param dispatcherTask instance of {@link TaskRef}
   */
  private void updateWarehouse(
      Identifier warehouseName,
      CommandsQueueRepository commandsQueueRepository,
      TaskRef dispatcherTask) {
    dispatcherTask.alterWarehouse(warehouseName.toSqlString());
    commandsQueueRepository.add(
        UPDATE_WAREHOUSE, new Variant(Map.of("warehouse_name", warehouseName.toSqlString())));
  }

  /**
   * Validates whether any instance to update in registry repository has active status.
   *
   * @param instanceSchema task reactor schema name
   * @throws TaskReactorInstanceIsActiveException If any of task reactor instances is in status
   *     active/
   */
  private void validateInstanceIsNotActive(String instanceSchema) {
    var instanceRegistry =
        instanceRegistryRepository.fetchAll().stream()
            .filter(instance -> instance.instanceName().getName().equals(instanceSchema))
            .findFirst()
            .orElseThrow(() -> new InstanceNotFoundException(instanceSchema));
    if (instanceRegistry.isActive()) {
      throw new TaskReactorInstanceIsActiveException(instanceSchema);
    }
  }
}
