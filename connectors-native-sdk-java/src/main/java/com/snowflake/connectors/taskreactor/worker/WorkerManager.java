/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueueManager;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;

/** A set of utilities for worker management. */
public class WorkerManager {

  private static final Logger LOG = TaskReactorLogger.getLogger(WorkerManager.class);

  private final WorkerStatusRepository workerStatusRepository;
  private final WorkerRegistryService workerRegistryService;
  private final WorkerCombinedView workerCombinedView;
  private final WorkerQueueManager workerQueueManager;
  private final WorkerTaskManager workerTaskManager;

  /**
   * Creates a new {@link WorkerManager}.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   * @return new manager instance
   */
  public static WorkerManager from(Session session, Identifier instanceSchema) {
    return new WorkerManager(
        WorkerStatusRepository.getInstance(session, instanceSchema),
        WorkerRegistryService.from(session, instanceSchema),
        WorkerCombinedView.getInstance(session, instanceSchema),
        WorkerQueueManager.getInstance(session, instanceSchema),
        WorkerTaskManager.from(session, instanceSchema));
  }

  WorkerManager(
      WorkerStatusRepository workerStatusRepository,
      WorkerRegistryService workerRegistryService,
      WorkerCombinedView workerCombinedView,
      WorkerQueueManager workerQueueManager,
      WorkerTaskManager workerTaskManager) {
    this.workerStatusRepository = workerStatusRepository;
    this.workerRegistryService = workerRegistryService;
    this.workerCombinedView = workerCombinedView;
    this.workerQueueManager = workerQueueManager;
    this.workerTaskManager = workerTaskManager;
  }

  /** Recalculates the number of active workers. */
  public void reconcileWorkersNumber() {
    LOG.debug("Reconciling workers number.");
    List<WorkerId> workersToProvision = workerRegistryService.getWorkersToProvision();
    List<WorkerId> workersToDelete = workerRegistryService.getWorkersToDelete();

    boolean provisionedWorkers = provisionWorkers(workersToProvision);
    boolean deletedWorkers = deleteWorkers(workersToDelete);

    if (deletedWorkers
        || provisionedWorkers
        || workerRegistryService.getActiveWorkers().isEmpty()) {
      List<WorkerId> workerIds = workerRegistryService.getActiveWorkers();
      workerCombinedView.recreate(workerIds);
    }
  }

  private boolean provisionWorkers(List<WorkerId> workerIds) {
    if (workerIds.isEmpty()) {
      LOG.debug("There are no workers to be provisioned.");
      return false;
    }

    workerIds.stream()
        .filter(workerRegistryService::canProvisionWorker)
        .forEach(this::provisionWorker);

    return true;
  }

  private void provisionWorker(WorkerId workerId) {
    LOG.debug("Provisioning worker with id {}", workerId);
    workerQueueManager.createWorkerQueueIfNotExist(workerId);
    workerTaskManager.createWorkerTask(workerId);
    workerStatusRepository.updateStatusFor(workerId, AVAILABLE);
    workerRegistryService.workerProvisioned(workerId);
  }

  private boolean deleteWorkers(List<WorkerId> workerIds) {
    if (workerIds.isEmpty()) {
      LOG.debug("There are no workers to be deleted.");
      return false;
    }

    Set<WorkerId> availableWorkers = workerStatusRepository.getAvailableWorkers();
    workerIds.stream()
        .filter(availableWorkers::contains)
        .filter(workerRegistryService::canDeleteWorker)
        .forEach(this::deleteWorker);

    return true;
  }

  private void deleteWorker(WorkerId workerId) {
    LOG.debug("Deleting worker with id {}", workerId);
    workerTaskManager.dropWorkerTask(workerId);
    workerQueueManager.dropWorkerQueue(workerId);
    workerStatusRepository.removeStatusFor(workerId);
    workerRegistryService.workerDeleted(workerId);
  }
}
