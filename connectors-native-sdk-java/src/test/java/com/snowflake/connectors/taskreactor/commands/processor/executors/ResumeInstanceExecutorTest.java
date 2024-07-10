/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.InMemoryTaskManagement;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.worker.InMemoryWorkerTaskManagerProvider;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerTaskManager;
import com.snowflake.connectors.taskreactor.worker.registry.InMemoryWorkerRegistry;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatus;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ResumeInstanceExecutorTest {

  private static final Identifier INSTANCE_SCHEMA = Identifier.from("SCHEMA");

  private final InMemoryInstanceRegistryRepository instanceRegistryRepository =
      new InMemoryInstanceRegistryRepository();
  private final InMemoryTaskManagement taskRepository = new InMemoryTaskManagement();
  private final WorkerTaskManager workerTaskManager =
      InMemoryWorkerTaskManagerProvider.getInstance(INSTANCE_SCHEMA, taskRepository);
  private final InMemoryWorkerStatusRepository workerStatusRepository =
      new InMemoryWorkerStatusRepository();
  private final InMemoryWorkerRegistry workerRegistry = new InMemoryWorkerRegistry();
  private final WorkerRegistryService workerRegistryService =
      new WorkerRegistryService(new InMemoryWorkerStatusRepository(), workerRegistry);

  private final ResumeInstanceExecutor executor =
      new ResumeInstanceExecutor(
          instanceRegistryRepository,
          workerTaskManager,
          workerStatusRepository,
          workerRegistryService,
          INSTANCE_SCHEMA);

  @AfterEach
  void cleanup() {
    instanceRegistryRepository.clear();
    taskRepository.clear();
    workerStatusRepository.clear();
    workerRegistry.clear();
  }

  @Test
  void shouldResumeOnlyActiveAndUpForDeletionWorkers() {
    // given
    instanceRegistryRepository.addInstance(INSTANCE_SCHEMA, false);

    workerExistsInWorkerRegistry(1, WorkerLifecycleStatus.REQUESTED);
    workerExistsInWorkerRegistry(2, WorkerLifecycleStatus.PROVISIONING);
    pausedWorkerExists(3, WorkerLifecycleStatus.UP_FOR_DELETION);
    workerExistsInWorkerRegistry(4, WorkerLifecycleStatus.DELETING);
    workerExistsInWorkerRegistry(5, WorkerLifecycleStatus.DELETED);
    pausedWorkerExists(6, WorkerLifecycleStatus.ACTIVE);

    // when
    executor.execute(new Command("id", RESUME_INSTANCE.name(), null, 1));

    // then
    assertWorkerTaskIsResumed(3);
    assertWorkerTaskIsResumed(6);
    assertThat(instanceRegistryRepository.fetch(INSTANCE_SCHEMA).isActive()).isTrue();
  }

  @Test
  void shouldResumeOnlyWorkAssignedAndInProgressAndScheduledForCancellationWorkers() {
    // given
    instanceRegistryRepository.addInstance(INSTANCE_SCHEMA, false);

    pausedWorkerExists(1, WorkerStatus.WORK_ASSIGNED);
    pausedWorkerExists(2, WorkerStatus.IN_PROGRESS);
    pausedWorkerExists(3, WorkerStatus.AVAILABLE);
    pausedWorkerExists(4, WorkerStatus.SCHEDULED_FOR_CANCELLATION);

    // when
    executor.execute(new Command("id", RESUME_INSTANCE.name(), null, 1));

    // then
    assertWorkerTaskIsResumed(1);
    assertWorkerTaskIsResumed(2);
    assertWorkerTaskIsResumed(4);
    assertThat(instanceRegistryRepository.fetch(INSTANCE_SCHEMA).isActive()).isTrue();
  }

  private void workerExistsInWorkerRegistry(int workerId, WorkerLifecycleStatus status) {
    workerRegistry.insertWorkers(1);
    workerRegistry.setWorkersStatus(status, List.of(new WorkerId(workerId)));
  }

  private void pausedWorkerExists(int workerId, WorkerLifecycleStatus status) {
    pausedWorkerExists(workerId, status, WorkerStatus.WORK_ASSIGNED);
  }

  private void pausedWorkerExists(int workerId, WorkerStatus workerStatus) {
    pausedWorkerExists(workerId, WorkerLifecycleStatus.ACTIVE, workerStatus);
  }

  private void pausedWorkerExists(
      int workerId, WorkerLifecycleStatus lifecycleStatus, WorkerStatus workerStatus) {
    WorkerId id = new WorkerId(workerId);
    workerExistsInWorkerRegistry(workerId, lifecycleStatus);
    workerStatusRepository.updateStatusFor(id, workerStatus);
    suspendedTaskExists(ObjectName.from(INSTANCE_SCHEMA, ComponentNames.workerTask(id)));
  }

  private void suspendedTaskExists(ObjectName name) {
    taskRepository.create(
        new TaskDefinition(
            new TaskProperties.Builder(name, "def", "1 MINUTE").withState("suspended").build()),
        false,
        false);
  }

  private void assertWorkerTaskIsResumed(int workerId) {
    ObjectName workerTask =
        ObjectName.from(INSTANCE_SCHEMA, ComponentNames.workerTask(new WorkerId(workerId)));
    assertThat(taskRepository.fetch(workerTask).fetch()).isStarted();
  }
}
