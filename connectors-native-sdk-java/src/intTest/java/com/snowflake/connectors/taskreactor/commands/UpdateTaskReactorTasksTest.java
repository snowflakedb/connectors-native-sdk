/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands;

import static com.snowflake.connectors.taskreactor.ComponentNames.COMMANDS_QUEUE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskReactorInstanceIsActiveException;
import com.snowflake.connectors.common.task.UpdateTaskReactorTasks;
import com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest;
import com.snowflake.connectors.taskreactor.registry.TaskReactorInstance;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class UpdateTaskReactorTasksTest extends BaseTaskReactorIntegrationTest {

  private static final TaskReactorTestInstance instance =
      TaskReactorTestInstance.buildFromScratch(TEST_INSTANCE, session)
          .withQueue()
          .withCommandsQueue()
          .withWorkerRegistry()
          .withWorkerStatus()
          .createInstance();

  @AfterEach
  void cleanUp() {
    session.table(ObjectName.from(TEST_INSTANCE, COMMANDS_QUEUE).getEscapedName()).delete();
  }

  @AfterAll
  static void dropInstance() {
    instance.delete();
  }

  @Test
  void shouldNotUpdateWhenInstanceIsActive() {
    // given
    addToRegistry(new TaskReactorInstance(Identifier.from(TEST_INSTANCE), true, true));

    // then
    assertThatThrownBy(
            () ->
                UpdateTaskReactorTasks.getInstance(session)
                    .updateInstance(TEST_INSTANCE, Identifier.from("test")))
        .isInstanceOf(TaskReactorInstanceIsActiveException.class)
        .hasMessage(
            String.format(
                "Instance '%s' shouldn't be resumed or started, when updating warehouse.",
                TEST_INSTANCE));
  }

  private void addToRegistry(TaskReactorInstance instance) {
    session
        .sql(
            String.format(
                "INSERT INTO TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY (INSTANCE_NAME,"
                    + " IS_INITIALIZED, IS_ACTIVE) VALUES ('%s', %s, %s)",
                instance.instanceName().toSqlString(),
                instance.isInitialized(),
                instance.isActive()))
        .collect();
  }
}
