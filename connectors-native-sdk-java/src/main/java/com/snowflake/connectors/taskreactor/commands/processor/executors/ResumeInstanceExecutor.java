/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.IN_PROGRESS;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.WORK_ASSIGNED;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerTaskManager;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Executor of the {@code RESUME_INSTANCE} command. */
public class ResumeInstanceExecutor implements CommandExecutor {

  private static final List<WorkerStatus> WORKER_STATUSES_TO_RESUME =
      List.of(WORK_ASSIGNED, SCHEDULED_FOR_CANCELLATION, IN_PROGRESS);

  private final InstanceRegistryRepository instanceRegistryRepository;
  private final WorkerTaskManager workerTaskManager;
  private final WorkerStatusRepository workerStatusRepository;
  private final WorkerRegistryService workerRegistryService;
  private final Identifier instanceName;

  ResumeInstanceExecutor(
      InstanceRegistryRepository instanceRegistryRepository,
      WorkerTaskManager workerTaskManager,
      WorkerStatusRepository workerStatusRepository,
      WorkerRegistryService workerRegistryService,
      Identifier instanceName) {
    this.instanceRegistryRepository = instanceRegistryRepository;
    this.workerTaskManager = workerTaskManager;
    this.workerStatusRepository = workerStatusRepository;
    this.workerRegistryService = workerRegistryService;
    this.instanceName = instanceName;
  }

  /**
   * Creates new instance of {@link ResumeInstanceExecutor}
   *
   * @param session session
   * @param instanceName name of Task Reactor instance
   * @return new ResumeInstanceExecutor object
   */
  public static ResumeInstanceExecutor getInstance(Session session, Identifier instanceName) {
    return new ResumeInstanceExecutor(
        InstanceRegistryRepository.getInstance(session),
        WorkerTaskManager.from(session, instanceName),
        WorkerStatusRepository.getInstance(session, instanceName),
        WorkerRegistryService.from(session, instanceName),
        instanceName);
  }

  /**
   * Resumes an instance of Task Reactor. It starts all Task Reactor's active worker tasks and
   * changes isActive flag in INSTANCE_REGISTRY table.
   *
   * @param command command to be executed - for RESUME_INSTANCE command no payload is needed
   */
  @Override
  public void execute(Command command) {
    workerRegistryService.getActiveOrUpForDeletionWorkers().forEach(this::resumeIfNeeded);
    instanceRegistryRepository.setActive(instanceName);
  }

  private void resumeIfNeeded(WorkerId workerId) {
    WorkerStatus workerStatus = workerStatusRepository.getStatusFor(workerId);
    if (WORKER_STATUSES_TO_RESUME.contains(workerStatus)) {
      workerTaskManager.resumeWorkerTask(workerId);
    }
  }
}
