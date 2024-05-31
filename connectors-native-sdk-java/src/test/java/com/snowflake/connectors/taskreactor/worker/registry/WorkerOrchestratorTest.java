/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.DELETED;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.REQUESTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.entry;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class WorkerOrchestratorTest {
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final InMemoryWorkerRegistry workerRegistry = new InMemoryWorkerRegistry();
  private final WorkerRegistryService workerRegistryService =
      new WorkerRegistryService(workerStatusRepository, workerRegistry);
  private final WorkerOrchestrator workerOrchestrator =
      new WorkerOrchestrator(workerRegistryService);

  @AfterEach
  void afterEach() {
    workerStatusRepository.clear();
    workerRegistry.clear();
  }

  @Test
  void shouldThrowExceptionWhenWorkersNumberIsLowerThanZero() {
    assertThatExceptionOfType(WorkerOrchestratorValidationException.class)
        .isThrownBy(() -> workerOrchestrator.setWorkersNumber(-1));
  }

  @Test
  void shouldSkipModificationIfWorkerNumberIsEqualToArgumentValue() {
    // given
    workerRegistry.insertWorkers(2);

    // when
    String result = workerOrchestrator.setWorkersNumber(2);

    // then
    assertThat(result).isEqualTo("No-op. New workers number is the same as previous one.");
    assertThat(workerRegistry.store())
        .containsExactly(entry(new WorkerId(1), REQUESTED), entry(new WorkerId(2), REQUESTED));
  }

  @Test
  void shouldAddWorkers() {
    // when
    String result = workerOrchestrator.setWorkersNumber(2);

    // then
    assertThat(result).isEqualTo("Number of workers added: 2.");
    assertThat(workerRegistry.store())
        .containsExactly(entry(new WorkerId(1), REQUESTED), entry(new WorkerId(2), REQUESTED));
  }

  @Test
  void shouldAddWorkersNumberReducedByAlreadyRequestedWorkers() {
    // given
    workerRegistry.insertWorkers(1);

    // when
    String result = workerOrchestrator.setWorkersNumber(3);

    // then
    assertThat(result).isEqualTo("Number of workers added: 2.");
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), REQUESTED),
            entry(new WorkerId(2), REQUESTED),
            entry(new WorkerId(3), REQUESTED));
  }

  @Test
  void shouldDeleteWorkers() {
    // given
    workerRegistry.insertWorkers(3);

    // when
    String result = workerOrchestrator.setWorkersNumber(1);

    // then
    assertThat(result).isEqualTo("Number of workers scheduled for deletion: 2.");
    assertThat(workerRegistry.store())
        .containsExactly(
            entry(new WorkerId(1), REQUESTED),
            entry(new WorkerId(2), DELETED),
            entry(new WorkerId(3), DELETED));
  }
}
