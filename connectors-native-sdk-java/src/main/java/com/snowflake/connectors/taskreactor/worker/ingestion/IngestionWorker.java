/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;

/** A worker executing an {@link Ingestion}. */
public class IngestionWorker<T> extends Worker<T> {

  private final Ingestion<T> ingestion;

  /**
   * Creates a new {@link IngestionWorker}.
   *
   * @param session Snowpark session object
   * @param ingestion ingestion
   * @param workerId worker id
   * @param instanceSchema Task Reactor instance name
   * @param <T> ingestion state type
   * @return a new ingestion worker
   */
  public static <T> IngestionWorker<T> from(
      Session session, Ingestion<T> ingestion, WorkerId workerId, Identifier instanceSchema) {
    return new IngestionWorker<>(
        ingestion,
        workerId,
        instanceSchema,
        WorkerStatusRepository.getInstance(session, instanceSchema),
        ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId)),
        WorkerQueue.getInstance(session, instanceSchema),
        TaskRepository.getInstance(session));
  }

  IngestionWorker(
      Ingestion<T> ingestion,
      WorkerId workerId,
      Identifier instanceName,
      WorkerStatusRepository workerStatusRepository,
      ObjectName workerTask,
      WorkerQueue workerQueue,
      TaskRepository taskRepository) {
    super(workerId, instanceName, workerStatusRepository, workerTask, workerQueue, taskRepository);
    this.ingestion = ingestion;
  }

  @Override
  protected T performWork(WorkItem workItem) {
    T state = ingestion.initialState(workItem);
    ingestion.preIngestion(workItem);

    do {
      if (shouldCancel()) {
        ingestion.ingestionCancelled(workItem, state);
        return state;
      }

      state = ingestion.performIngestion(workItem, state);
    } while (!ingestion.isIngestionCompleted(workItem, state));

    ingestion.postIngestion(workItem, state);
    return state;
  }
}
