/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerTaskManager;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Executor of the {@code PAUSE_INSTANCE} command. */
public class PauseInstanceExecutor implements CommandExecutor {

  private final InstanceRegistryRepository instanceRegistryRepository;
  private final WorkerTaskManager workerTaskManager;
  private final DispatcherTaskManager dispatcherTaskManager;
  private final WorkerRegistryService workerRegistryService;
  private final Identifier instanceName;

  PauseInstanceExecutor(
      InstanceRegistryRepository instanceRegistryRepository,
      WorkerTaskManager workerTaskManager,
      DispatcherTaskManager dispatcherTaskManager,
      WorkerRegistryService workerRegistryService,
      Identifier instanceName) {
    this.instanceRegistryRepository = instanceRegistryRepository;
    this.workerTaskManager = workerTaskManager;
    this.dispatcherTaskManager = dispatcherTaskManager;
    this.workerRegistryService = workerRegistryService;
    this.instanceName = instanceName;
  }

  /**
   * Creates new instance of {@link PauseInstanceExecutor}.
   *
   * @param session Snowpark session object
   * @param instanceName name of the Task Reactor instance
   * @return new executor instance
   */
  public static PauseInstanceExecutor getInstance(Session session, Identifier instanceName) {
    return new PauseInstanceExecutor(
        InstanceRegistryRepository.getInstance(session),
        WorkerTaskManager.from(session, instanceName),
        DispatcherTaskManager.from(session, instanceName),
        WorkerRegistryService.from(session, instanceName),
        instanceName);
  }

  /**
   * Pauses an instance of Task Reactor. It suspends all Task Reactor tasks: worker tasks and
   * dispatcher task and changes isActive flag in INSTANCE_REGISTRY table.
   *
   * @param command command to be executed - for PAUSE_INSTANCE command no payload is needed
   */
  @Override
  public void execute(Command command) {
    List<WorkerId> workers = workerRegistryService.getAllWorkers();

    workers.forEach(workerTaskManager::suspendWorkerTaskIfExists);
    dispatcherTaskManager.suspendDispatcherTask();

    instanceRegistryRepository.setInactive(instanceName);
  }
}
