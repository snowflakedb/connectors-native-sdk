/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.COMMAND;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.InMemoryTaskManagement;
import com.snowflake.connectors.taskreactor.InMemoryTaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultUpdateTaskReactorTasksTest {

  private static final Identifier INSTANCE_NAME = Identifier.from("START_INSTANCE");

  private static final TaskProperties DISPATCHER_TASK_PROPERTIES =
      new TaskProperties.Builder(
              ObjectName.from(INSTANCE_NAME.getValue(), "DISPATCHER_TASK"),
              "SELECT 1",
              "USING CRON * * * * * UTC")
          .withWarehouse("XS")
          .build();

  private InstanceRegistryRepository instanceRegistryRepository;
  private InMemoryTaskReactorInstanceComponentProvider componentProvider;
  private TaskRepository taskRepository;
  private UpdateTaskReactorTasks updateTaskReactorTasks;
  private TaskReactorInstanceActionExecutor instanceExecutor;

  @BeforeEach
  void setUp() {
    var instanceRegistry = new InMemoryInstanceRegistryRepository();
    instanceRegistry.addInstance(INSTANCE_NAME, false);
    this.instanceRegistryRepository = instanceRegistry;

    var task = new InMemoryTaskManagement();
    task.create(
        new TaskDefinition(DISPATCHER_TASK_PROPERTIES, new TaskParameters(new HashMap<>())),
        false,
        false);
    this.componentProvider = new InMemoryTaskReactorInstanceComponentProvider();
    this.taskRepository = task;
    this.instanceExecutor =
        new TaskReactorInstanceActionExecutor(() -> true, instanceRegistryRepository);
    this.updateTaskReactorTasks =
        new DefaultUpdateTaskReactorTasks(
            instanceRegistryRepository, componentProvider, taskRepository, instanceExecutor);
  }

  @Test
  void shouldChangeWarehouse() {
    // given
    var warehouse = Identifier.from("TEST_WAREHOUSE");
    initializeUpdateTaskReactorTasks();

    // when
    updateTaskReactorTasks.update(warehouse);

    // then
    var task = taskRepository.fetch(DISPATCHER_TASK_PROPERTIES.name());
    var commandsQueue = componentProvider.commandsQueueRepositories();
    // this part should be changed after new identifiers approach implementation
    assertThat(task.fetch().warehouse())
        .isEqualTo(Identifier.from(warehouse.getValue()).getValue());
    assertThat(commandsQueue).containsKey(INSTANCE_NAME);
    var commands = commandsQueue.get(INSTANCE_NAME).fetchAllSupportedOrderedBySeqNo();
    assertThat(commands)
        .hasSize(1)
        .singleElement(COMMAND)
        .hasCommandType(Command.CommandType.UPDATE_WAREHOUSE)
        .hasPayload(new Variant(Map.of("warehouse_name", warehouse.getValue())));
  }

  @Test
  void shouldNotChangeWarehouse() {
    // given
    var warehouse = Identifier.from("TEST_WAREHOUSE");
    var instanceRegistry = new InMemoryInstanceRegistryRepository();
    instanceRegistry.addInstance(INSTANCE_NAME, true);
    this.instanceRegistryRepository = instanceRegistry;
    initializeUpdateTaskReactorTasks();

    // then
    assertThatThrownBy(() -> updateTaskReactorTasks.update(warehouse))
        .hasMessageContaining(
            format(
                "Instance '%s' shouldn't be resumed or started, when updating warehouse.",
                INSTANCE_NAME.getValue()));
  }

  private void initializeUpdateTaskReactorTasks() {
    this.updateTaskReactorTasks =
        new DefaultUpdateTaskReactorTasks(
            instanceRegistryRepository, componentProvider, taskRepository, instanceExecutor);
  }
}
