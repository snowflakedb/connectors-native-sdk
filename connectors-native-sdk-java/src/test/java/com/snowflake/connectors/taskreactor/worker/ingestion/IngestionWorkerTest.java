/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.WorkerException;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerJobCancelledException;
import com.snowflake.connectors.taskreactor.worker.queue.InMemoryWorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IngestionWorkerTest {
  private final Ingestion<String> ingestion = mock();
  private final WorkerId workerId = new WorkerId(1);
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final ObjectName workerTask = ObjectName.from("SCHEMA", "WORKER_1");
  private final Identifier instanceName = Identifier.from("INSTANCE_NAME");
  private final InMemoryWorkerQueue workerQueue = new InMemoryWorkerQueue();
  private final TaskRepository taskRepository = mock();
  private final TaskRef taskRef = mock();
  private final Worker<String> ingestionWorker =
      new IngestionWorker<>(
          ingestion,
          workerId,
          instanceName,
          workerStatusRepository,
          workerTask,
          workerQueue,
          taskRepository);

  @BeforeEach
  void beforeEach() {
    when(taskRepository.fetch(workerTask)).thenReturn(taskRef);
    workerQueue.push(getQueueItem(), workerId);
  }

  @AfterEach
  void afterEach() {
    Mockito.reset(taskRepository);
    Mockito.reset(taskRef);
  }

  @Test
  void shouldSkipExecutingWhenWorkerIsScheduledForCancellation() {
    // given
    workerStatusRepository.updateStatusFor(workerId, WorkerStatus.SCHEDULED_FOR_CANCELLATION);

    // when
    assertThatExceptionOfType(WorkerJobCancelledException.class)
        .isThrownBy(ingestionWorker::run)
        .withMessage("Running worker was scheduled for cancellation.");

    // then
    verify(ingestion, never()).postIngestion(any(), any());
    verify(ingestion, never()).performIngestion(any(), any());

    verify(taskRef).suspend();
    assertThat(workerQueue.store()).isEmpty();
    assertThat(workerStatusRepository.getAvailableWorkers()).containsExactly(workerId);
  }

  @Test
  void shouldThrowExceptionDuringIngestion() {
    // given
    IllegalStateException exception = new IllegalStateException("Initial state failure.");
    when(ingestion.initialState(any())).thenThrow(exception);

    // then
    assertThatExceptionOfType(WorkerException.class)
        .isThrownBy(ingestionWorker::run)
        .withMessage("Worker WorkerId[id = 1] failed")
        .withCause(exception);

    verify(ingestion, never()).postIngestion(any(), any());
    verify(ingestion, never()).performIngestion(any(), any());

    verify(taskRef).suspend();
    assertThat(workerQueue.store()).isEmpty();
    assertThat(workerStatusRepository.getAvailableWorkers()).containsExactly(workerId);
  }

  @Test
  void shouldPerformIngestion() throws WorkerException {
    when(ingestion.initialState(any())).thenReturn("OK");
    when(ingestion.isIngestionCompleted(any(), any())).thenReturn(true);

    // when
    ingestionWorker.run();

    // then
    verify(ingestion).postIngestion(any(), any());
    verify(ingestion).performIngestion(any(), any());

    verify(taskRef).suspend();
    assertThat(workerQueue.store()).isEmpty();
    assertThat(workerStatusRepository.getAvailableWorkers()).containsExactly(workerId);
  }

  private static QueueItem getQueueItem() {
    return QueueItem.fromMap(
        Map.of(
            "id", new Variant("id"),
            "timestamp", new Variant(Timestamp.from(Instant.now())),
            "resourceId", new Variant("resource-id"),
            "workerPayload", new Variant("payload")));
  }
}
