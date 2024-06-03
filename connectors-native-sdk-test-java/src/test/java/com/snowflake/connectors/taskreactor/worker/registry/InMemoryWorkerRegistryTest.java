/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryWorkerRegistryTest {
  InMemoryWorkerRegistry registry;

  @BeforeEach
  void beforeEach() {
    registry = new InMemoryWorkerRegistry();
  }

  @Test
  void shouldInsertWorkersIntoEmtpyRegistry() {
    // expect
    registry.insertWorkers(3);

    assertThat(registry.store())
        .containsExactly(
            entry(new WorkerId(1), WorkerLifecycleStatus.REQUESTED),
            entry(new WorkerId(2), WorkerLifecycleStatus.REQUESTED),
            entry(new WorkerId(3), WorkerLifecycleStatus.REQUESTED));
  }

  @Test
  void shouldInsertWorkersIntoNotEmptyRegistry() {
    // given
    registry.insertWorkers(1);

    // when
    registry.insertWorkers(2);

    // then
    assertThat(registry.store())
        .containsExactly(
            entry(new WorkerId(1), WorkerLifecycleStatus.REQUESTED),
            entry(new WorkerId(2), WorkerLifecycleStatus.REQUESTED),
            entry(new WorkerId(3), WorkerLifecycleStatus.REQUESTED));
  }

  @Test
  void shouldSetWorkersStatuses() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    registry.insertWorkers(1);

    // when
    registry.setWorkersStatus(WorkerLifecycleStatus.UP_FOR_DELETION, List.of(workerId1, workerId2));

    // then
    assertThat(registry.store())
        .containsExactly(entry(workerId1, WorkerLifecycleStatus.UP_FOR_DELETION));
  }

  @Test
  void shouldUpdateWorkersStatuses() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    registry.insertWorkers(1);
    registry.store().put(workerId2, WorkerLifecycleStatus.DELETED);

    // when
    long updated =
        registry.updateWorkersStatus(
            WorkerLifecycleStatus.ACTIVE,
            WorkerLifecycleStatus.REQUESTED,
            List.of(workerId1, workerId2));

    // then
    assertThat(updated).isEqualTo(1);
    assertThat(registry.store())
        .containsExactly(
            entry(workerId1, WorkerLifecycleStatus.ACTIVE),
            entry(workerId2, WorkerLifecycleStatus.DELETED));
  }

  @Test
  void shouldGetWorkersInStatuses() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    WorkerId workerId3 = new WorkerId(3);
    registry.store().put(workerId1, WorkerLifecycleStatus.ACTIVE);
    registry.store().put(workerId2, WorkerLifecycleStatus.REQUESTED);
    registry.store().put(workerId3, WorkerLifecycleStatus.DELETED);

    // when
    List<WorkerId> workerIds =
        registry.getWorkerIds(WorkerLifecycleStatus.ACTIVE, WorkerLifecycleStatus.REQUESTED);

    // then
    assertThat(workerIds).containsExactly(workerId1, workerId2);
  }

  @Test
  void shouldGetWorkerCountInStatuses() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    WorkerId workerId3 = new WorkerId(3);
    registry.store().put(workerId1, WorkerLifecycleStatus.ACTIVE);
    registry.store().put(workerId2, WorkerLifecycleStatus.REQUESTED);
    registry.store().put(workerId3, WorkerLifecycleStatus.DELETED);

    // when
    int count =
        registry.getWorkerCountWithStatuses(
            WorkerLifecycleStatus.ACTIVE, WorkerLifecycleStatus.REQUESTED);

    // then
    assertThat(count).isEqualTo(2);
  }
}
