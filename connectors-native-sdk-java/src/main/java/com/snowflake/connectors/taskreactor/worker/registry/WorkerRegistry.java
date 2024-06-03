/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Session;
import java.util.Collection;
import java.util.List;

/** Registry repository of task reactor workers. */
public interface WorkerRegistry {

  /**
   * Requests given amount of workers to be created.
   *
   * @param workersToInsert numbers of workers to be created.
   */
  void insertWorkers(int workersToInsert);

  /**
   * Changes status of workers to provided one.
   *
   * @param status new status of workers
   * @param workerIds workers identifiers
   */
  void setWorkersStatus(WorkerLifecycleStatus status, List<WorkerId> workerIds);

  /**
   * Changes workers status to new one on condition that they are currently in given status.
   *
   * @param newStatus status to be changed to
   * @param currentStatus current status
   * @param workerIds worker identifiers
   * @return count of worker statuses modified
   */
  long updateWorkersStatus(
      WorkerLifecycleStatus newStatus,
      WorkerLifecycleStatus currentStatus,
      Collection<WorkerId> workerIds);

  /**
   * Fetches workers which are in given status.
   *
   * @param statuses collection of statuses
   * @return Worker identifiers
   */
  List<WorkerId> getWorkerIds(WorkerLifecycleStatus... statuses);

  /**
   * Fetches amount of workers with given states.
   *
   * @param statuses collection of statuses
   * @return Count of workers in given statuses
   */
  int getWorkerCountWithStatuses(WorkerLifecycleStatus... statuses);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses:
   *
   * <ul>
   *   <li>a default implementation of {@link
   *       com.snowflake.connectors.taskreactor.worker.registry.DefaultWorkerRegistry
   *       DefaultWorkerRegistry}, created for the {@code <schema>.WORKER_REGISTRY} table.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new repository instance
   */
  static WorkerRegistry getInstance(Session session, Identifier schema) {
    return new DefaultWorkerRegistry(session, schema);
  }
}
