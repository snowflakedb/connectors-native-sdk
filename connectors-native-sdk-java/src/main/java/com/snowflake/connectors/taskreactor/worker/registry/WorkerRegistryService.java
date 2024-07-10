/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.ACTIVE;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.DELETED;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.DELETING;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.PROVISIONING;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.REQUESTED;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.UP_FOR_DELETION;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;

/** Registry of task reactor workers. */
public class WorkerRegistryService {

  private static final Logger LOG = TaskReactorLogger.getLogger(WorkerRegistryService.class);

  private final WorkerStatusRepository statusRepository;
  private final WorkerRegistry workerRegistry;

  /**
   * Creates a new {@link WorkerRegistryService}.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   * @return a new service instance
   */
  public static WorkerRegistryService from(Session session, Identifier instanceSchema) {
    return new WorkerRegistryService(
        WorkerStatusRepository.getInstance(session, instanceSchema),
        WorkerRegistry.getInstance(session, instanceSchema));
  }

  /**
   * Creates a new {@link WorkerRegistryService}.
   *
   * @param statusRepository Snowpark session object
   * @param workerRegistry task reactor instance schema name
   */
  public WorkerRegistryService(
      WorkerStatusRepository statusRepository, WorkerRegistry workerRegistry) {
    this.statusRepository = statusRepository;
    this.workerRegistry = workerRegistry;
  }

  public List<WorkerId> getAllWorkers() {
    return workerRegistry.getWorkerIds(ACTIVE, PROVISIONING, UP_FOR_DELETION, DELETING);
  }

  /**
   * Returns ids of active workers.
   *
   * @return list of ids of active workers
   */
  public List<WorkerId> getActiveWorkers() {
    return workerRegistry.getWorkerIds(ACTIVE);
  }

  /**
   * Returns the number of active and requested workers.
   *
   * @return number of active and requested workers
   */
  public int getActiveOrRequestedWorkersCount() {
    return workerRegistry.getWorkerCountWithStatuses(ACTIVE, REQUESTED);
  }

  /**
   * Returns ids of requested workers.
   *
   * @return list of ids of requested workers
   */
  public List<WorkerId> getWorkersToProvision() {
    return workerRegistry.getWorkerIds(REQUESTED);
  }

  /**
   * Returns ids of active, provisioning or up for deletion workers.
   *
   * @return list of ids of active, provisioning or up for deletion workers
   */
  public List<WorkerId> getActiveProvisioningOrUpForDeletionWorkers() {
    return workerRegistry.getWorkerIds(ACTIVE, PROVISIONING, UP_FOR_DELETION);
  }

  /**
   * Returns ids of active or up for deletion workers.
   *
   * @return list of ids of active or up for deletion workers
   */
  public List<WorkerId> getActiveOrUpForDeletionWorkers() {
    return workerRegistry.getWorkerIds(ACTIVE, UP_FOR_DELETION);
  }

  /**
   * Returns whether a worker with the specified id can be requested.
   *
   * @param workerId worker id
   * @return whether a worker with the specified id can be requested
   */
  public boolean canProvisionWorker(WorkerId workerId) {
    return workerRegistry.updateWorkersStatus(PROVISIONING, REQUESTED, List.of(workerId)) == 1;
  }

  /**
   * Sets the status of the specified worker to {@code ACTIVE}.
   *
   * @param workerId worker id
   */
  public void workerProvisioned(WorkerId workerId) {
    workerRegistry.setWorkersStatus(ACTIVE, List.of(workerId));
  }

  /**
   * Returns ids of workers waiting for deletion.
   *
   * @return list of ids of workers waiting for deletion
   */
  public List<WorkerId> getWorkersToDelete() {
    return workerRegistry.getWorkerIds(UP_FOR_DELETION);
  }

  /**
   * Returns whether a worker with the specified id can be deleted.
   *
   * @param workerId worker id
   * @return whether a worker with the specified id can be deleted
   */
  public boolean canDeleteWorker(WorkerId workerId) {
    return workerRegistry.updateWorkersStatus(DELETING, UP_FOR_DELETION, List.of(workerId)) == 1;
  }

  /**
   * Sets the status of the specified worker to {@code DELETED}.
   *
   * @param workerId worker id
   */
  public void workerDeleted(WorkerId workerId) {
    workerRegistry.setWorkersStatus(DELETED, List.of(workerId));
  }

  /**
   * Adds the specified numbers of workers to the instance.
   *
   * @param workersToAdd number of workers to add
   */
  public void addNewWorkers(int workersToAdd) {
    LOG.debug("Attempting to add {} workers", workersToAdd);
    long restoredWorkers = restoreUpForDeletionWorkers(workersToAdd);
    int workersToInsert = workersToAdd - (int) restoredWorkers;
    if (workersToInsert > 0) {
      workerRegistry.insertWorkers(workersToInsert);
    }
  }

  /**
   * Removes specified numbers of workers to the instance.
   *
   * @param workersToDelete number of workers to delete
   */
  public void deleteWorkers(int workersToDelete) {
    LOG.debug("Attempting to delete {} workers", workersToDelete);
    int deletedRequestedWorkersCount = deleteRequestedWorkers(workersToDelete);
    int activeWorkersToDelete = workersToDelete - deletedRequestedWorkersCount;

    deleteActiveWorkers(activeWorkersToDelete);
  }

  private long restoreUpForDeletionWorkers(int workersToAdd) {
    List<WorkerId> workerIdsUpForDeletion =
        workerRegistry.getWorkerIds(UP_FOR_DELETION).stream().limit(workersToAdd).collect(toList());

    return workerRegistry.updateWorkersStatus(ACTIVE, UP_FOR_DELETION, workerIdsUpForDeletion);
  }

  private int deleteRequestedWorkers(int workersToDelete) {
    List<WorkerId> workersToProvision = workerRegistry.getWorkerIds(REQUESTED);
    if (!workersToProvision.isEmpty()) {
      List<WorkerId> workerIds =
          workersToProvision.stream()
              .sorted(Comparator.comparing(WorkerId::value).reversed())
              .limit(workersToDelete)
              .collect(toList());
      workerRegistry.setWorkersStatus(DELETED, workerIds);
    }
    return workersToProvision.size();
  }

  private void deleteActiveWorkers(int activeWorkersToDelete) {
    if (activeWorkersToDelete < 1) {
      LOG.debug("No active workers will be deleted.");
      return;
    }
    Set<WorkerId> availableWorkers = statusRepository.getAvailableWorkers();
    List<WorkerId> workerIdsToDelete =
        workerRegistry.getWorkerIds(ACTIVE).stream()
            .sorted(prioritizeAvailableWorkersComparator(availableWorkers))
            .limit(activeWorkersToDelete)
            .collect(toList());

    workerRegistry.setWorkersStatus(UP_FOR_DELETION, workerIdsToDelete);
  }

  @SuppressWarnings("ComparatorMethodParameterNotUsed")
  private Comparator<WorkerId> prioritizeAvailableWorkersComparator(
      Set<WorkerId> availableWorkers) {
    return (first, second) ->
        availableWorkers.contains(first) && !availableWorkers.contains(second) ? -1 : 1;
  }
}
