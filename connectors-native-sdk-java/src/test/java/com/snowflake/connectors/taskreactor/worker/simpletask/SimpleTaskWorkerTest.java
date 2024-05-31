/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.simpletask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.WorkerException;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.queue.InMemoryWorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SimpleTaskWorkerTest {

  private final SimpleTask<ConnectorResponse> simpleTask = mock();
  private final WorkerId workerId = new WorkerId(1);
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final ObjectName workerTask = ObjectName.from("SCHEMA", "WORKER_1");
  private final Identifier instanceName = Identifier.from("INSTANCE_NAME");
  private final InMemoryWorkerQueue workerQueue = new InMemoryWorkerQueue();
  private final TaskRepository taskRepository = mock();
  private final TaskRef taskRef = mock();
  private final Worker<ConnectorResponse> simpleTaskWorker =
      new SimpleTaskWorker<>(
          simpleTask,
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
  void shouldExecuteSimpleTask() throws WorkerException {
    when(simpleTask.execute(any())).thenReturn(ConnectorResponse.success());

    // when
    simpleTaskWorker.run();

    // then
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
