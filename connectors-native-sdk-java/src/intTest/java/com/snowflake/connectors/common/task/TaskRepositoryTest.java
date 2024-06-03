/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TaskRepositoryTest extends BaseIntegrationTest {

  static TaskRepository taskRepository;
  static TaskDefinition defaultTaskDefinition;
  static TaskProperties defaultTaskProperties;
  static final ObjectName taskName = ObjectName.from("PUBLIC", "TASK_NAME");
  static final int RANDOM_ID = new Random().nextInt(Integer.MAX_VALUE);
  static final String QUOTED_WAREHOUSE =
      String.format("\"OTHER_SP3CI@L_QUOT3D_WAREHOUSE_%d\"", RANDOM_ID);

  @BeforeAll
  static void beforeAll() {
    defaultTaskProperties = new Builder(taskName, "select 1", "1 minute").build();
    defaultTaskDefinition = new TaskDefinition(defaultTaskProperties);
    taskRepository = new DefaultTaskRepository(session);
    createWarehouse();
  }

  @AfterAll
  static void afterAll() {
    dropWarehouse();
  }

  @Test
  void shouldCreateTaskReferenceWithoutCallingDatabase() {
    DefaultTaskRepository repository = new DefaultTaskRepository(null);

    TaskRef result = repository.fetch(taskName);

    assertThat(result.name()).isEqualTo(taskName);
  }

  @Test
  void shouldCreateTaskInDatabase() {
    TaskRef result = taskRepository.create(defaultTaskDefinition, false, false);

    assertThat(result.name()).isEqualTo(taskName);

    // cleanup
    result.drop();
  }

  @Test
  void shouldReplaceTaskWithTheSameNameInDatabase() {
    TaskRef firstTaskRef = taskRepository.create(defaultTaskDefinition, false, false);

    TaskRef replacedTaskRef = taskRepository.create(defaultTaskDefinition, true, false);

    assertThat(firstTaskRef.name()).isEqualTo(replacedTaskRef.name());

    // cleanup
    firstTaskRef.drop();
  }

  @Test
  void shouldThrowExceptionTryingToCreateTaskWithTheSameNameInDatabase() {
    TaskRef firstTaskRef = taskRepository.create(defaultTaskDefinition, false, false);

    assertThatThrownBy(() -> taskRepository.create(defaultTaskDefinition, false, false))
        .isInstanceOf(TaskCreationException.class)
        .hasMessage("SQL compilation error:\nObject 'PUBLIC.TASK_NAME' already exists.");

    // cleanup
    firstTaskRef.drop();
  }

  @Test
  void shouldCreateTaskWithParametersInDatabase() {
    Map<String, String> parameters =
        Map.of(
            "SUSPEND_TASK_AFTER_NUM_FAILURES", "5",
            "AGGREGATE_XP_STATS_ACROSS_STEPS", "Enable");
    TaskParameters taskParameters = new TaskParameters(parameters);
    TaskDefinition taskDefinition = new TaskDefinition(defaultTaskProperties, taskParameters);

    TaskRef result = taskRepository.create(taskDefinition, false, false);

    assertThat(result.name()).isEqualTo(taskName);

    // cleanup
    result.drop();
  }

  @Test
  void shouldFailToCreateTaskWithIncorrectCustomParameter() {
    Map<String, String> parameters = Map.of("AGGREGATE_XP_STATS_ACROSS_STEPS", "XXX");
    TaskParameters taskParameters = new TaskParameters(parameters);
    TaskDefinition taskDefinition = new TaskDefinition(defaultTaskProperties, taskParameters);

    assertThatThrownBy(() -> taskRepository.create(taskDefinition, false, false))
        .isInstanceOf(TaskCreationException.class)
        .hasMessage(
            "SQL compilation error:\n"
                + "invalid value [XXX] for parameter 'AGGREGATE_XP_STATS_ACROSS_STEPS'");
  }

  @Test
  void shouldCreateTaskWithQuotedWarehouse() {
    TaskProperties taskProperties =
        new Builder(taskName, "select 1", "1 minute").withWarehouse(QUOTED_WAREHOUSE).build();
    TaskDefinition taskDefinition = new TaskDefinition(taskProperties);

    TaskRef result = taskRepository.create(taskDefinition, false, false);

    assertThat(result.name()).isEqualTo(taskName);

    // cleanup
    result.drop();
  }

  private static void createWarehouse() {
    session.sql(String.format("create or replace warehouse %s", QUOTED_WAREHOUSE)).collect();
  }

  private static void dropWarehouse() {
    session.sql(String.format("drop warehouse if exists %s", QUOTED_WAREHOUSE)).collect();
  }
}
