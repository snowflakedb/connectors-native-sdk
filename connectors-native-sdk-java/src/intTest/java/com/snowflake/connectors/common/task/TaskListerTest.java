/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties.Builder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TaskListerTest extends BaseIntegrationTest {

  static TaskLister taskLister;
  static TaskRepository taskRepository;
  static TaskRef taskRef1;
  static TaskRef taskRef2;

  @BeforeAll
  static void beforeAll() {
    taskRepository = new DefaultTaskRepository(session);
    taskLister = new DefaultTaskLister(session);

    ObjectName firstTaskName = ObjectName.from("PUBLIC", "FOO");
    ObjectName secondTaskName = ObjectName.from("PUBLIC", "POO");

    TaskProperties taskProperties1 = new Builder(firstTaskName, "select 1", "1 minute").build();
    TaskProperties taskProperties2 = new Builder(secondTaskName, "select 1", "1 minute").build();
    TaskDefinition taskDefinition1 = new TaskDefinition(taskProperties1);
    TaskDefinition taskDefinition2 = new TaskDefinition(taskProperties2);

    taskRef1 = taskRepository.create(taskDefinition1, false, false);
    taskRef2 = taskRepository.create(taskDefinition2, false, false);
  }

  @AfterAll
  static void afterAll() {
    taskRef1.drop();
    taskRef2.drop();
  }

  @Test
  public void shouldListTasksInSchema() {
    // expect
    assertThat(taskLister.showTasks("PUBLIC"))
        .hasSize(2)
        .extracting(TaskProperties::name)
        .contains(taskRef1.name(), taskRef2.name());
  }

  @Test
  public void shouldListTasksLikeFOOInSchema() {
    // expect
    assertThat(taskLister.showTasks("PUBLIC", "FOO"))
        .hasSize(1)
        .extracting(TaskProperties::name)
        .contains(taskRef1.name());
  }

  @Test
  public void shouldNotListAnyTasks() {
    // expect
    assertThat(taskLister.showTasks("PUBLIC", "UNKNOWN_TASK_NAME")).isEmpty();
  }

  @Test
  public void shouldShowSpecificTask() {
    // expect
    assertThat(taskLister.showTask(taskRef1.name()))
        .isPresent()
        .get()
        .extracting(TaskProperties::name)
        .isEqualTo(taskRef1.name());
  }

  @Test
  public void shouldNotShowAnyTask() {
    // expect
    assertThat(taskLister.showTask(ObjectName.from("PUBLIC", "UNKNOWN_TASK_NAME"))).isNotPresent();
  }

  @Test
  void shouldHandleTaskWithSpecialCharacters() {
    // expect
    assertThatNoException().isThrownBy(() -> taskLister.showTasks("PUBLIC", "a'b\"c❄d"));
  }
}
