/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.IN_PROGRESS;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.WORK_ASSIGNED;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WorkerStatusRepositoryIntegrationTest extends BaseTaskReactorIntegrationTest {

  private static final int WORKER_ID = 11;
  private static final String INSTANCE_NAME = "TEST_INSTANCE";

  private WorkerStatusRepository repository;
  private TaskReactorTestInstance instance;

  @BeforeAll
  void prepareWorkerStatusTable() {
    repository = new DefaultWorkerStatusRepository(session, Identifier.from(INSTANCE_NAME));
    instance =
        TaskReactorTestInstance.buildFromScratch(INSTANCE_NAME, session)
            .withWorkerStatus()
            .createInstance();
  }

  @AfterAll
  void dropInstance() {
    instance.delete();
  }

  @AfterEach
  void truncateTable() {
    session.sql(String.format("TRUNCATE TABLE %s.WORKER_STATUS", INSTANCE_NAME)).collect();
  }

  @Test
  void shouldReturnEmptyWhenWorkerWasNeverInAvailableStatus() {
    // given
    insertWorkerStatusRow(2, AVAILABLE, now());
    insertWorkerStatusRow(WORKER_ID, WORK_ASSIGNED, now());

    // when
    Optional<Instant> timestamp = repository.getLastAvailable(new WorkerId(WORKER_ID));

    // then
    assertThat(timestamp).isEmpty();
  }

  @Test
  void shouldReturnTheLastTimestampOfWorkerWithGivenIdInAvailableStatus() {
    // given
    var now = now().truncatedTo(MILLIS);
    insertWorkerStatusRow(WORKER_ID, AVAILABLE, now.minusSeconds(10));
    insertWorkerStatusRow(WORKER_ID, AVAILABLE, now);
    insertWorkerStatusRow(WORKER_ID, WORK_ASSIGNED, now.plusSeconds(10));
    insertWorkerStatusRow(WORKER_ID, IN_PROGRESS, now.plusSeconds(20));
    insertWorkerStatusRow(2, AVAILABLE, now.plusSeconds(30));

    // when
    Optional<Instant> timestamp = repository.getLastAvailable(new WorkerId(WORKER_ID));

    // then
    assertThat(timestamp).isPresent().hasValue(now);
  }

  @Test
  void shouldReturnWorkerNumberForEachStatus() {
    // given
    var now = now();
    insertWorkerStatusRow(1, AVAILABLE, now.minusSeconds(10));
    insertWorkerStatusRow(1, IN_PROGRESS, now);

    insertWorkerStatusRow(2, AVAILABLE, now);
    insertWorkerStatusRow(3, AVAILABLE, now);
    insertWorkerStatusRow(4, WORK_ASSIGNED, now);
    insertWorkerStatusRow(5, SCHEDULED_FOR_CANCELLATION, now);

    insertWorkerStatusRow(6, AVAILABLE, now);
    insertWorkerStatusRow(6, WORK_ASSIGNED, now.plusSeconds(1));
    insertWorkerStatusRow(6, IN_PROGRESS, now.plusSeconds(2));
    insertWorkerStatusRow(6, AVAILABLE, now.plusSeconds(3));

    // when
    Map<String, Integer> result = repository.getWorkersNumberForEachStatus();

    // then
    assertThat(result)
        .hasSize(4)
        .containsEntry(AVAILABLE.name(), 3)
        .containsEntry(WORK_ASSIGNED.name(), 1)
        .containsEntry(IN_PROGRESS.name(), 1)
        .containsEntry(SCHEDULED_FOR_CANCELLATION.name(), 1);
  }

  @Test
  void shouldReturnNoWorkerNumberEntryWhenNoWorkerHasGivenStatus() {
    // when
    Map<String, Integer> result = repository.getWorkersNumberForEachStatus();

    // then
    assertThat(result).isEmpty();
  }

  private void insertWorkerStatusRow(int workerId, WorkerStatus status, Instant timestamp) {
    session
        .sql(
            String.format(
                "INSERT INTO %s.WORKER_STATUS (WORKER_ID, STATUS, TIMESTAMP) VALUES (%s, '%s',"
                    + " '%s')",
                INSTANCE_NAME, workerId, status.name(), timestamp.toEpochMilli()))
        .toLocalIterator();
  }
}
