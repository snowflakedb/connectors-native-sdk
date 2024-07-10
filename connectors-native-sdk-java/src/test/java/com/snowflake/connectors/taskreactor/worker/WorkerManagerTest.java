/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.config.InMemoryConfigRepository;
import com.snowflake.connectors.taskreactor.worker.queue.InMemoryWorkerQueueManager;
import com.snowflake.connectors.taskreactor.worker.registry.InMemoryWorkerRegistry;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class WorkerManagerTest {

  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final InMemoryWorkerRegistry workerRegistry = new InMemoryWorkerRegistry();
  private final WorkerRegistryService workerRegistryService =
      new WorkerRegistryService(workerStatusRepository, workerRegistry);
  private final InMemoryWorkerCombinedView workerCombinedView =
      spy(new InMemoryWorkerCombinedView());
  private final InMemoryWorkerQueueManager workerQueueManager = new InMemoryWorkerQueueManager();
  private final Identifier instanceSchema = Identifier.from("SCHEMA");
  private final InMemoryConfigRepository configRepository = new InMemoryConfigRepository();
  private final TaskRepository taskRepository = mock();
  private final WorkerTaskManager workerTaskManager =
      new WorkerTaskManager(instanceSchema, configRepository, taskRepository);

  private final WorkerManager workerManager =
      new WorkerManager(
          workerStatusRepository,
          workerRegistryService,
          workerCombinedView,
          workerQueueManager,
          workerTaskManager);

  @BeforeEach
  void beforeEach() {
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC-test",
            "WORK_SELECTOR_TYPE", "PROCEDURE",
            "WORK_SELECTOR", "work-selector-test",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "WAREHOUSE", "\"\\\"escap3d%_warehouse\\\"\""));
  }

  @AfterEach
  void afterEach() {
    Mockito.reset(taskRepository);
    workerRegistry.clear();
    workerQueueManager.clear();
    workerStatusRepository.clear();
    configRepository.clear();
  }

  @Test
  void shouldRecreateViewWhenThereAreNoActiveWorkers() {
    // when
    workerManager.reconcileWorkersNumber();

    // then
    verify(workerCombinedView).recreate(anyList());
    assertThat(workerRegistry.store()).isEmpty();
  }

  @Test
  void shouldNotRecreateViewWhenThereAreActiveWorkers() {
    // given
    workerRegistry.insertWorkers(1);
    workerRegistry.updateWorkersStatus(
        WorkerLifecycleStatus.ACTIVE, WorkerLifecycleStatus.REQUESTED, List.of(new WorkerId(1)));

    // when
    workerManager.reconcileWorkersNumber();

    // then
    verify(workerCombinedView, never()).recreate(anyList());
  }

  @Test
  void shouldProvisionWorkers() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);

    workerRegistry.insertWorkers(2);

    // when
    workerManager.reconcileWorkersNumber();

    // then
    assertThat(workerQueueManager.workerQueues()).containsExactly(workerId1, workerId2);
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(workerId1, WorkerLifecycleStatus.ACTIVE),
            entry(workerId2, WorkerLifecycleStatus.ACTIVE));
    assertThat(workerStatusRepository.store())
        .containsExactly(
            entry(workerId1, WorkerStatus.AVAILABLE), entry(workerId2, WorkerStatus.AVAILABLE));

    verify(workerCombinedView).recreate(anyList());
    verify(taskRepository, times(2)).create(any(), eq(false), eq(true));
  }

  @Test
  void shouldDeleteWorkers() {
    // given
    TaskRef taskRef = mock();
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    WorkerId workerId3 = new WorkerId(3);

    workerRegistry.insertWorkers(3);
    workerRegistry.setWorkersStatus(
        WorkerLifecycleStatus.UP_FOR_DELETION, List.of(workerId1, workerId2));
    workerRegistry.setWorkersStatus(WorkerLifecycleStatus.ACTIVE, List.of(workerId3));
    workerStatusRepository.updateStatusesFor(WorkerStatus.AVAILABLE, List.of(workerId2, workerId3));
    workerStatusRepository.updateStatusFor(workerId1, WorkerStatus.IN_PROGRESS);
    workerQueueManager.createWorkerQueueIfNotExist(workerId1);
    workerQueueManager.createWorkerQueueIfNotExist(workerId2);
    workerQueueManager.createWorkerQueueIfNotExist(workerId3);

    when(taskRepository.fetch(any())).thenReturn(taskRef);

    // when
    workerManager.reconcileWorkersNumber();

    // then
    assertThat(workerQueueManager.workerQueues()).containsExactly(workerId1, workerId3);
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(workerId1, WorkerLifecycleStatus.UP_FOR_DELETION),
            entry(workerId2, WorkerLifecycleStatus.DELETED),
            entry(workerId3, WorkerLifecycleStatus.ACTIVE));
    assertThat(workerStatusRepository.store())
        .containsExactly(
            entry(workerId1, WorkerStatus.IN_PROGRESS), entry(workerId3, WorkerStatus.AVAILABLE));

    verify(workerCombinedView).recreate(anyList());
    verify(taskRef, times(1)).drop();
  }
}
