/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;

/** A Task Reactor worker orchestrator. */
public class WorkerOrchestrator {

  private final WorkerRegistryService workerRegistryService;

  /**
   * Creates a new {@link WorkerOrchestrator}.
   *
   * @param session Snowpark session object
   * @param instanceSchema Task Reactor instance name
   * @return a new worker orchestrator
   */
  public static WorkerOrchestrator from(Session session, Identifier instanceSchema) {
    return new WorkerOrchestrator(WorkerRegistryService.from(session, instanceSchema));
  }

  WorkerOrchestrator(WorkerRegistryService workerRegistryService) {
    this.workerRegistryService = workerRegistryService;
  }

  /**
   * Sets the number of workers for the Task Reactor instance.
   *
   * @param workersNumber new number of workers
   * @return whether the workers number was changed
   */
  public String setWorkersNumber(int workersNumber) {
    if (workersNumber < 0) {
      throw new WorkerOrchestratorValidationException("Number of workers cannot be negative.");
    }

    int currentWorkersNumber = workerRegistryService.getActiveOrRequestedWorkersCount();

    if (workersNumber > currentWorkersNumber) {
      int workersToAdd = workersNumber - currentWorkersNumber;
      workerRegistryService.addNewWorkers(workersToAdd);
      return String.format("Number of workers added: %d.", workersToAdd);
    } else if (workersNumber < currentWorkersNumber) {
      int workersToDelete = currentWorkersNumber - workersNumber;
      workerRegistryService.deleteWorkers(workersToDelete);
      return String.format("Number of workers scheduled for deletion: %d.", workersToDelete);
    }
    return "No-op. New workers number is the same as previous one.";
  }
}
