/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.ACTIVE;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.DELETED;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.REQUESTED;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.UP_FOR_DELETION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class WorkerRegistryServiceTest {
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final InMemoryWorkerRegistry workerRegistry = new InMemoryWorkerRegistry();
  private final WorkerRegistryService workerRegistryService =
      new WorkerRegistryService(workerStatusRepository, workerRegistry);

  @AfterEach
  void afterEach() {
    workerStatusRepository.clear();
    workerRegistry.clear();
  }

  @Test
  void shouldAddNewWorkers() {
    // when
    workerRegistryService.addNewWorkers(3);

    // then
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), REQUESTED),
            entry(new WorkerId(2), REQUESTED),
            entry(new WorkerId(3), REQUESTED));
  }

  @Test
  void shouldAddNewWorkersWhenThereAreUpForDeletionWorkers() {
    // given
    workerRegistry.insertWorkers(2);
    workerRegistry.updateWorkersStatus(
        UP_FOR_DELETION, REQUESTED, List.of(new WorkerId(1), new WorkerId(2)));

    // when
    workerRegistryService.addNewWorkers(5);

    // then
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), ACTIVE),
            entry(new WorkerId(2), ACTIVE),
            entry(new WorkerId(3), REQUESTED),
            entry(new WorkerId(4), REQUESTED),
            entry(new WorkerId(5), REQUESTED));
  }

  @Test
  void shouldDeleteRequestedWorkers() {
    // given
    workerRegistry.insertWorkers(2);

    // when
    workerRegistryService.deleteWorkers(5);

    // then
    assertThat(workerRegistry.store())
        .containsExactly(entry(new WorkerId(1), DELETED), entry(new WorkerId(2), DELETED));
  }

  @Test
  void shouldDeleteActiveWorkers() {
    // given
    workerRegistry.insertWorkers(3);
    workerRegistry.updateWorkersStatus(
        ACTIVE, REQUESTED, List.of(new WorkerId(1), new WorkerId(2)));

    // when
    workerRegistryService.deleteWorkers(5);

    // then
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), UP_FOR_DELETION),
            entry(new WorkerId(2), UP_FOR_DELETION),
            entry(new WorkerId(3), DELETED));
  }

  @Test
  void shouldPrioritizeActiveWorkers() {
    // given
    workerRegistry.insertWorkers(3);
    workerRegistry.updateWorkersStatus(
        ACTIVE, REQUESTED, List.of(new WorkerId(1), new WorkerId(2), new WorkerId(3)));
    workerStatusRepository.updateStatusesFor(
        WorkerStatus.AVAILABLE, List.of(new WorkerId(2), new WorkerId(3)));

    // when
    workerRegistryService.deleteWorkers(2);

    // then
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), ACTIVE),
            entry(new WorkerId(2), UP_FOR_DELETION),
            entry(new WorkerId(3), UP_FOR_DELETION));
  }
}
