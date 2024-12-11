/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TaskRefTest extends BaseIntegrationTest {

  private static final ObjectName taskName = ObjectName.from("PUBLIC", "TASK_NAME");
  private static final ObjectName invalidTaskName = ObjectName.from("PUBLIC", "INVALID");
  private static final Identifier quotedWarehouse = Identifier.from("\"Qu0T3d_WH_N@m3\"");

  private static TaskRepository taskRepository;
  private static TaskRef invalidTaskRef;
  private static TaskDefinition taskDefinition;
  private static TaskProperties taskProperties;
  private static TaskParameters taskParameters;

  private TaskRef taskRef;

  @BeforeAll
  void beforeAll() {
    String query =
        format(
            "CREATE WAREHOUSE IF NOT EXISTS \"\"%s\"\" WAREHOUSE_SIZE=XSMALL",
            quotedWarehouse.getQuotedValue());
    session.sql(query).collect();
    taskProperties = new Builder(taskName, "SELECT 1", "1 MINUTE").build();
    Map<String, String> map = new HashMap<>();
    map.put("SUSPEND_TASK_AFTER_NUM_FAILURES", "5");
    map.put("AGGREGATE_XP_STATS_ACROSS_STEPS", "Enable");
    taskParameters = new TaskParameters(map);
    taskDefinition = new TaskDefinition(taskProperties, taskParameters);
    taskRepository = new DefaultTaskRepository(session);

    invalidTaskRef = taskRepository.fetch(invalidTaskName);

    session.sql("CREATE OR REPLACE WAREHOUSE " + quotedWarehouse.getValue()).toLocalIterator();
  }

  @BeforeEach
  void beforeEach() {
    taskRef = taskRepository.create(taskDefinition, false, false);
  }

  @AfterEach
  void afterEach() {
    taskRef.dropIfExists();
  }

  @Test
  void shouldReturnTaskObjectName() {
    // expect
    assertThat(taskRef.name()).isEqualTo(taskName);
  }

  @Test
  void shouldExecuteTaskWithoutFailure() {
    // expect
    assertThatNoException().isThrownBy(() -> taskRef.execute());
  }

  @Test
  void shouldThrowExceptionWhenExecutionInvalidTask() {
    // expect
    assertThatThrownBy(() -> invalidTaskRef.execute()).isInstanceOf(SnowflakeSQLException.class);
  }

  @Test
  void shouldResumeTask() {
    // when
    taskRef.resume();

    // then
    assertThat(taskRef.fetch()).hasState("started");
  }

  @Test
  void shouldSuspendTask() {
    // given
    taskRef.resume();

    // when
    taskRef.suspend();

    // then
    assertThat(taskRef.fetch()).hasState("suspended");
  }

  @Test
  void shouldFailToFetchAfterDroppingTask() {
    // when
    taskRef.drop();

    // then
    assertThatThrownBy(taskRef::fetch).isInstanceOf(SnowflakeSQLException.class);
  }

  @Test
  void shouldAlterTaskSchedule() {
    // when
    taskRef.alterSchedule("5 MINUTE");

    // then
    assertThat(taskRef.fetch()).hasSchedule("5 MINUTE");
  }

  @Test
  void shouldFailToAlterTaskDueToInvalidSchedule() {
    // expect
    assertThatThrownBy(() -> taskRef.alterSchedule("invalid"))
        .isInstanceOf(SnowflakeSQLException.class);
  }

  @Test
  void shouldAlterTaskWarehouse() {
    // when
    taskRef.alterWarehouse(quotedWarehouse.getValue());

    // then
    assertThat(taskRef.fetch()).hasWarehouse(quotedWarehouse.getValue());
  }

  @Test
  void shouldFailToAlterTaskDueToNonExistingWarehouse() {
    assertThatThrownBy(() -> taskRef.alterWarehouse("invalid"))
        .isInstanceOf(SnowflakeSQLException.class);
  }

  @Test
  public void shouldFetchTaskProperties() {
    // expect
    assertThat(taskRef.fetch())
        .hasObjectName(taskProperties.name())
        .hasDefinition(taskProperties.definition())
        .hasSchedule(taskProperties.schedule())
        .hasWarehouse(taskProperties.warehouse());
  }

  @Test
  void shouldCreateAndFetchTaskWithLowerCaseWarehouse() {
    // given
    var newTaskName = ObjectName.from("PUBLIC", "WAREHOUSE_LOWERCASE_TASK");
    var newProperties =
        new Builder(newTaskName, "select 1", "1 minute").withWarehouse(quotedWarehouse).build();
    var newTaskDefinition = new TaskDefinition(newProperties, taskParameters);

    // when
    var newTaskRef = taskRepository.create(newTaskDefinition, false, false);

    // expect
    assertThat(newTaskRef.fetch())
        .hasObjectName(newProperties.name())
        .hasDefinition(newProperties.definition())
        .hasSchedule(newProperties.schedule())
        .hasWarehouse(newProperties.warehouse());
  }

  @Test
  void shouldNotThrowErrorIfTaskDoesNotExistsAndIfExistsMethodsUsed() {
    // expect
    assertThatNoException()
        .isThrownBy(
            () -> {
              invalidTaskRef.resumeIfExists();
              invalidTaskRef.suspendIfExists();
              invalidTaskRef.dropIfExists();
              invalidTaskRef.alterScheduleIfExists("10 MINUTE");
              invalidTaskRef.alterWarehouseIfExists("SOME_WAREHOUSE");
            });
  }

  @Test
  void shouldReturnFalseWhenTaskDoesNotExist() {
    // expect
    assertThat(invalidTaskRef.checkIfExists()).isFalse();
  }

  @Test
  void shouldReturnTrueWhenTaskExists() {
    // expect
    assertThat(taskRef.checkIfExists()).isTrue();
  }
}
