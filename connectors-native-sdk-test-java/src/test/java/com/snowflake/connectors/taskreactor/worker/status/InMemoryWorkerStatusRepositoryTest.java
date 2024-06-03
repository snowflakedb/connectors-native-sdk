/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryWorkerStatusRepositoryTest {
  InMemoryWorkerStatusRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = new InMemoryWorkerStatusRepository();
  }

  @Test
  void shouldReturnAvailableWorkers() {
    // given
    repository.updateStatusFor(new WorkerId(1), WorkerStatus.AVAILABLE);
    repository.updateStatusFor(new WorkerId(2), WorkerStatus.SCHEDULED_FOR_CANCELLATION);
    repository.updateStatusFor(new WorkerId(3), WorkerStatus.IN_PROGRESS);
    repository.updateStatusFor(new WorkerId(4), WorkerStatus.AVAILABLE);

    // when
    Set<WorkerId> result = repository.getAvailableWorkers();

    // then
    assertThat(result).containsExactlyInAnyOrder(new WorkerId(1), new WorkerId(4));
  }

  @Test
  void shouldGetStatusForGivenWorkerId() {
    // given
    repository.updateStatusFor(new WorkerId(1), WorkerStatus.SCHEDULED_FOR_CANCELLATION);

    // when
    WorkerStatus result = repository.getStatusFor(new WorkerId(1));

    assertThat(result).isEqualTo(WorkerStatus.SCHEDULED_FOR_CANCELLATION);
  }

  @Test
  void shouldGetNullStatusForMissingWorkerId() {
    // expect
    WorkerStatus result = repository.getStatusFor(new WorkerId(1));
    assertThat(result).isNull();
  }

  @Test
  void shouldUpdateStatusForWorkerId() {
    // given
    WorkerId workerId = new WorkerId(1);
    repository.updateStatusFor(workerId, WorkerStatus.SCHEDULED_FOR_CANCELLATION);

    // when
    repository.updateStatusFor(workerId, WorkerStatus.IN_PROGRESS);

    // then
    assertThat(repository.getStatusFor(workerId)).isEqualTo(WorkerStatus.IN_PROGRESS);
  }

  @Test
  void shouldUpdateStatusesForWorkerIds() {
    // given
    WorkerId workerId1 = new WorkerId(1);
    WorkerId workerId2 = new WorkerId(2);
    repository.updateStatusFor(workerId1, WorkerStatus.SCHEDULED_FOR_CANCELLATION);
    repository.updateStatusFor(workerId2, WorkerStatus.AVAILABLE);

    // when
    repository.updateStatusesFor(WorkerStatus.IN_PROGRESS, List.of(workerId1, workerId2));

    // then
    assertThat(repository.store())
        .containsExactly(
            entry(workerId1, WorkerStatus.IN_PROGRESS), entry(workerId2, WorkerStatus.IN_PROGRESS));
  }

  @Test
  void shouldRemoveStatusForWorkerId() {
    // given
    WorkerId workerId = new WorkerId(1);
    repository.updateStatusFor(workerId, WorkerStatus.SCHEDULED_FOR_CANCELLATION);

    // when
    repository.removeStatusFor(workerId);

    // then
    assertThat(repository.store()).isEmpty();
  }
}
