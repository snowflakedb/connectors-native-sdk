/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.config.InMemoryConfigRepository;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class WorkerTaskManagerTest {

  private final Identifier instanceSchema = Identifier.from("SCHEMA");
  private final InMemoryConfigRepository configRepository = new InMemoryConfigRepository();
  private final TaskRepository taskRepository = mock();
  private final WorkerTaskManager workerTaskManager =
      new WorkerTaskManager(instanceSchema, configRepository, taskRepository);

  private final ArgumentCaptor<TaskDefinition> taskDefinitionCaptor = ArgumentCaptor.captor();

  @BeforeEach
  void beforeEach() {
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC",
            "WORK_SELECTOR_TYPE", "PROCEDURE",
            "WORK_SELECTOR", "work-selector-test",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "WAREHOUSE", "\"\\\"escap3d%_warehouse\\\"\""));
  }

  @AfterEach
  void afterEach() {
    Mockito.reset(taskRepository);
    configRepository.clear();
  }

  @Test
  void shouldCreateWorkerTask() {
    // given
    WorkerId workerId = new WorkerId(5);

    // when
    workerTaskManager.createWorkerTask(workerId);

    // then
    verify(taskRepository).create(taskDefinitionCaptor.capture(), eq(false), eq(true));
    assertThat(taskDefinitionCaptor.getValue().properties())
        .hasObjectName(ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId)))
        .hasDefinition("CALL TEST_PROC(5, 'SCHEMA')")
        .hasWarehouse("\"escap3d%_warehouse\"");
  }

  @Test
  void shouldDropWorker() {
    // given
    WorkerId workerId = new WorkerId(7);
    TaskRef taskRef = mock();
    ObjectName workerTask = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    when(taskRepository.fetch(workerTask)).thenReturn(taskRef);

    // when
    workerTaskManager.dropWorkerTask(workerId);

    // then
    verify(taskRef).drop();
  }
}
