/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import com.fasterxml.jackson.databind.JavaType;
import com.snowflake.connectors.application.observability.CrudIngestionRunRepository;
import com.snowflake.connectors.application.observability.DefaultIngestionRunRepository;
import com.snowflake.connectors.application.observability.IngestionRun;
import com.snowflake.connectors.application.observability.IngestionRunRepository;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;
import java.util.Optional;

/**
 * A worker executing an {@link SimpleIngestion}. * It executes common logic for most of the simple
 * ingestion scenarios.
 *
 * @param <I> resource id class, containing properties which identify the resource in the source
 *     system
 * @param <M> resource metadata class, containing additional properties which identify the resource
 *     in the source system. The properties can be fetched automatically or calculated by the
 *     connector
 * @param <C> custom ingestion configuration class, containing custom ingestion properties
 * @param <D> destination configuration class, containing properties describing where the ingested
 *     data should be stored
 */
public class SimpleIngestionWorker<I, M, D, C> extends Worker<ConnectorResponse> {

  private final SimpleIngestion<I, M, D, C> ingestion;
  private final IngestionRunRepository ingestionRunRepository;
  private final CrudIngestionRunRepository crudIngestionRunRepository;
  private final TransactionManager transactionManager;
  private final JavaType simpleIngestionWorkItemType;

  /**
   * Creates a new {@link SimpleIngestionWorker}.
   *
   * @param session Snowpark session object
   * @param ingestion simple ingestion
   * @param workerId worker id
   * @param instanceSchema Task Reactor instance name
   * @param simpleIngestionWorkItemType {@link JavaType} type of SimpleIngestionWorkItem
   * @param <I> resource id class, containing properties which identify the resource in the source
   *     system
   * @param <M> resource metadata class, containing additional properties which identify the
   *     resource in the source system. The properties can be fetched automatically or calculated by
   *     the connector
   * @param <C> custom ingestion configuration class, containing custom ingestion properties
   * @param <D> destination configuration class, containing properties describing where the ingested
   *     data should be stored
   * @return a new simple ingestion worker
   */
  public static <I, M, D, C> SimpleIngestionWorker<I, M, D, C> from(
      Session session,
      SimpleIngestion<I, M, D, C> ingestion,
      WorkerId workerId,
      Identifier instanceSchema,
      JavaType simpleIngestionWorkItemType) {
    DefaultIngestionRunRepository ingestionRunRepository =
        new DefaultIngestionRunRepository(session);
    return new SimpleIngestionWorker<>(
        ingestion,
        workerId,
        instanceSchema,
        WorkerStatusRepository.getInstance(session, instanceSchema),
        ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId)),
        WorkerQueue.getInstance(session, instanceSchema),
        TaskRepository.getInstance(session),
        ingestionRunRepository,
        ingestionRunRepository,
        TransactionManager.getInstance(session),
        simpleIngestionWorkItemType);
  }

  SimpleIngestionWorker(
      SimpleIngestion<I, M, D, C> ingestion,
      WorkerId workerId,
      Identifier instanceName,
      WorkerStatusRepository workerStatusRepository,
      ObjectName workerTask,
      WorkerQueue workerQueue,
      TaskRepository taskRepository,
      IngestionRunRepository ingestionRunRepository,
      CrudIngestionRunRepository crudIngestionRunRepository,
      TransactionManager transactionManager,
      JavaType simpleIngestionWorkItemType) {
    super(workerId, instanceName, workerStatusRepository, workerTask, workerQueue, taskRepository);
    this.ingestion = ingestion;
    this.ingestionRunRepository = ingestionRunRepository;
    this.crudIngestionRunRepository = crudIngestionRunRepository;
    this.transactionManager = transactionManager;
    this.simpleIngestionWorkItemType = simpleIngestionWorkItemType;
  }

  protected ConnectorResponse performWork(WorkItem workItem) {
    SimpleIngestionWorkItem<I, M, D, C> simpleIngestionWorkItem =
        SimpleIngestionWorkItem.from(workItem, simpleIngestionWorkItemType);
    String ingestionRunId = startRun(simpleIngestionWorkItem);
    try {
      ingestionRunScope(simpleIngestionWorkItem, ingestionRunId);
    } catch (Exception e) {
      failIngestionRun(simpleIngestionWorkItem, ingestionRunId);
      throw e;
    }
    ingestion.postIngestion(simpleIngestionWorkItem, ingestionRunId);
    return ConnectorResponse.success();
  }

  private void ingestionRunScope(
      SimpleIngestionWorkItem<I, M, D, C> simpleIngestionWorkItem, String ingestionRunId) {
    ingestion.fetchData(simpleIngestionWorkItem, ingestionRunId);
    transactionManager.withTransaction(
        () -> {
          long numberOfRowsIngested =
              ingestion.persistData(simpleIngestionWorkItem, ingestionRunId);
          ingestionRunRepository.endRun(
              ingestionRunId, IngestionRun.IngestionStatus.COMPLETED, numberOfRowsIngested, null);
          ingestion.onSuccessfulIngestionCallback(simpleIngestionWorkItem, ingestionRunId);
        });
  }

  private void failIngestionRun(
      SimpleIngestionWorkItem<I, M, D, C> simpleIngestionWorkItem, String ingestionRunId) {
    transactionManager.withTransaction(
        () -> {
          ingestionRunRepository.endRun(
              ingestionRunId, IngestionRun.IngestionStatus.FAILED, 0L, null);
          ingestion.onFailedIngestionCallback(simpleIngestionWorkItem, ingestionRunId);
        });
  }

  private String startRun(SimpleIngestionWorkItem<I, M, D, C> ingestionWorkItem) {
    Optional<IngestionRun> ongoingIngestionRun =
        crudIngestionRunRepository
            .findOngoingIngestionRuns(ingestionWorkItem.getResourceIngestionDefinitionId())
            .stream()
            .findFirst();

    if (ongoingIngestionRun.isEmpty()) {
      return ingestionRunRepository.startRun(
          ingestionWorkItem.getResourceIngestionDefinitionId(),
          ingestionWorkItem.getIngestionConfigurationId(),
          ingestionWorkItem.getProcessId(),
          null);
    }
    return ongoingIngestionRun.get().getId();
  }
}
