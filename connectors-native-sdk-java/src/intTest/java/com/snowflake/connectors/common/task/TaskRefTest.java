/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import com.snowflake.connectors.util.sql.SqlStringFormatter;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TaskRefTest extends BaseIntegrationTest {

  static final ObjectName taskName = ObjectName.from("PUBLIC", "TASK_NAME");
  static final ObjectName invalidTaskName = ObjectName.from("PUBLIC", "INVALID");
  private static final String warehouseName = "CONNECTORS_NATIVE_SDK_J@V@_IT_WH_" + TEST_ID;
  private static final String warehouseLowercaseName = "true_lower_case" + TEST_ID;
  static TaskRepository taskRepository;
  static TaskRef invalidTaskRef;
  static TaskDefinition taskDefinition;
  static TaskProperties taskProperties;
  static TaskParameters taskParameters;
  private TaskRef taskRef;

  @BeforeAll
  static void beforeAll() {
    taskProperties = new Builder(taskName, "SELECT 1", "1 MINUTE").build();
    Map<String, String> map = new HashMap<>();
    map.put("SUSPEND_TASK_AFTER_NUM_FAILURES", "5");
    map.put("AGGREGATE_XP_STATS_ACROSS_STEPS", "Enable");
    taskParameters = new TaskParameters(map);
    taskDefinition = new TaskDefinition(taskProperties, taskParameters);
    taskRepository = new DefaultTaskRepository(session);

    invalidTaskRef = taskRepository.fetch(invalidTaskName);

    session.sql("CREATE OR REPLACE WAREHOUSE \"" + warehouseName + "\"").toLocalIterator();
    session.sql("CREATE OR REPLACE WAREHOUSE \"" + warehouseLowercaseName + "\"").toLocalIterator();
  }

  @AfterAll
  static void afterAll() {
    session.sql("DROP WAREHOUSE IF EXISTS \"" + warehouseName + "\"").toLocalIterator();
    session.sql("DROP WAREHOUSE IF EXISTS \"" + warehouseLowercaseName + "\"").toLocalIterator();
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
    // given
    var escapedWarehouse = SqlStringFormatter.escapeIdentifier(warehouseName);

    // when
    taskRef.alterWarehouse(escapedWarehouse);

    // then
    assertThat(taskRef.fetch()).hasWarehouse(escapedWarehouse);
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
        new Builder(newTaskName, "select 1", "1 minute")
            .withWarehouse(warehouseLowercaseName)
            .build();
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
