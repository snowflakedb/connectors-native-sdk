/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TaskRepositoryTest extends BaseIntegrationTest {

  private final ObjectName baseTaskName = ObjectName.from("PUBLIC", "TASK_NAME");
  private final ObjectName childTaskName = ObjectName.from("PUBLIC", "TASK_NAME_CHILD");
  private final int RANDOM_ID = new Random().nextInt(Integer.MAX_VALUE);
  private final String QUOTED_WAREHOUSE =
      format("\"OTHER_SP3CI@L_QUOT3D_WAREHOUSE_%d\"", RANDOM_ID);

  private TaskProperties defaultTaskProperties;
  private TaskDefinition defaultTaskDefinition;
  private TaskRepository taskRepository;

  @BeforeAll
  void beforeAll() {
    defaultTaskProperties = new Builder(baseTaskName, "select 1", "1 minute").build();
    defaultTaskDefinition = new TaskDefinition(defaultTaskProperties);
    taskRepository = new DefaultTaskRepository(session);
  }

  @Test
  void shouldCreateTaskReferenceWithoutCallingDatabase() {
    // given
    DefaultTaskRepository repository = new DefaultTaskRepository(null);

    // when
    TaskRef result = repository.fetch(baseTaskName);

    // then
    assertThat(result.name()).isEqualTo(baseTaskName);
  }

  @Test
  void shouldCreateTaskInDatabase() {
    // when
    TaskRef result = taskRepository.create(defaultTaskDefinition, false, false);

    // then
    assertThat(result.name()).isEqualTo(baseTaskName);

    // cleanup
    result.drop();
  }

  @Test
  void shouldReplaceTaskWithTheSameNameInDatabase() {
    // given
    TaskRef baseTask = taskRepository.create(defaultTaskDefinition, false, false);

    // when
    TaskRef replacedTask = taskRepository.create(defaultTaskDefinition, true, false);

    // then
    assertThat(baseTask.name()).isEqualTo(replacedTask.name());

    // cleanup
    replacedTask.drop();
  }

  @Test
  void shouldThrowExceptionTryingToCreateTaskWithTheSameNameInDatabase() {
    // given
    TaskRef baseTask = taskRepository.create(defaultTaskDefinition, false, false);

    // expect
    assertThatThrownBy(() -> taskRepository.create(defaultTaskDefinition, false, false))
        .isInstanceOf(TaskCreationException.class)
        .hasMessageContaining(format("Object '%s' already exists", baseTaskName.getValue()));

    // cleanup
    baseTask.drop();
  }

  @Test
  void shouldCreateTaskWithParametersInDatabase() {
    // given
    Map<String, String> parameters =
        Map.of(
            "SUSPEND_TASK_AFTER_NUM_FAILURES", "5",
            "AGGREGATE_XP_STATS_ACROSS_STEPS", "Enable");
    TaskParameters taskParameters = new TaskParameters(parameters);
    TaskDefinition taskDefinition = new TaskDefinition(defaultTaskProperties, taskParameters);

    // when
    TaskRef result = taskRepository.create(taskDefinition, false, false);

    // then
    assertThat(result.name()).isEqualTo(baseTaskName);

    // cleanup
    result.drop();
  }

  @Test
  void shouldFailToCreateTaskWithIncorrectCustomParameter() {
    // given
    Map<String, String> parameters = Map.of("AGGREGATE_XP_STATS_ACROSS_STEPS", "XXX");
    TaskParameters taskParameters = new TaskParameters(parameters);
    TaskDefinition taskDefinition = new TaskDefinition(defaultTaskProperties, taskParameters);

    // expect
    assertThatThrownBy(() -> taskRepository.create(taskDefinition, false, false))
        .isInstanceOf(TaskCreationException.class)
        .hasMessage(
            "SQL compilation error:\n"
                + "invalid value [XXX] for parameter 'AGGREGATE_XP_STATS_ACROSS_STEPS'");
  }

  @Test
  void shouldCreateTaskWithQuotedWarehouse() {
    // given
    TaskProperties taskProperties =
        new Builder(baseTaskName, "select 1", "1 minute").withWarehouse(QUOTED_WAREHOUSE).build();
    TaskDefinition taskDefinition = new TaskDefinition(taskProperties);
    createQuotedWarehouse();

    // when
    TaskRef result = taskRepository.create(taskDefinition, false, false);

    // then
    assertThat(result.name()).isEqualTo(baseTaskName);

    // cleanup
    result.drop();
    dropQuotedWarehouse();
  }

  @Test
  void shouldCreateTaskWithAfterParameter() {
    // given
    var baseTaskProps = new Builder(baseTaskName, "select 1", "1 minute").build();
    var baseTask = taskRepository.create(new TaskDefinition(baseTaskProps), false, false);

    // when
    var childTaskProps =
        new Builder(childTaskName, "select 1", null)
            .withPredecessors(List.of(taskRepository.fetch(this.baseTaskName)))
            .withCondition("true")
            .build();
    var childTask = taskRepository.create(new TaskDefinition(childTaskProps), false, false);

    // then
    var fetchedChildTaskProps = taskRepository.fetch(childTaskName).fetch();
    assertThat(fetchedChildTaskProps).hasPredecessors(List.of(addDbToTaskName(baseTask)));

    // cleanup
    childTask.drop();
    baseTask.drop();
  }

  private void createQuotedWarehouse() {
    session.sql(format("CREATE OR REPLACE WAREHOUSE %s", QUOTED_WAREHOUSE)).collect();
    session.sql(format("USE WAREHOUSE %s", QUOTED_WAREHOUSE)).collect();
  }

  private void dropQuotedWarehouse() {
    session.sql(format("DROP WAREHOUSE IF EXISTS %s", QUOTED_WAREHOUSE)).collect();
    session.sql(format("USE WAREHOUSE %s", WAREHOUSE_NAME)).collect();
  }

  private TaskRef addDbToTaskName(TaskRef taskRef) {
    var newName =
        ObjectName.from(
            Identifier.from(DATABASE_NAME),
            taskRef.name().getSchema().orElseThrow(),
            taskRef.name().getName());
    return TaskRef.of(session, newName);
  }
}
