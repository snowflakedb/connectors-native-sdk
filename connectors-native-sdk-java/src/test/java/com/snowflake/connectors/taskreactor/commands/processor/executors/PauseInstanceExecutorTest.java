/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.taskreactor.ComponentNames.DISPATCHER_TASK;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.InMemoryTaskRepository;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.connectors.taskreactor.dispatcher.InMemoryDispatcherTaskProvider;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import com.snowflake.connectors.taskreactor.worker.InMemoryWorkerTaskManagerProvider;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerTaskManager;
import com.snowflake.connectors.taskreactor.worker.registry.InMemoryWorkerRegistry;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerRegistryService;
import com.snowflake.connectors.taskreactor.worker.status.InMemoryWorkerStatusRepository;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PauseInstanceExecutorTest {

  private static final Identifier INSTANCE_SCHEMA = Identifier.fromWithAutoQuoting("SCHEMA");

  private final InMemoryInstanceRegistryRepository instanceRegistryRepository =
      new InMemoryInstanceRegistryRepository();
  private final TaskRepository taskRepository = new InMemoryTaskRepository();
  private final WorkerTaskManager workerTaskManager =
      InMemoryWorkerTaskManagerProvider.getInstance(INSTANCE_SCHEMA, taskRepository);
  private final DispatcherTaskManager dispatcherTaskManager =
      InMemoryDispatcherTaskProvider.getInstance(INSTANCE_SCHEMA, taskRepository);
  private final InMemoryWorkerRegistry workerRegistry = new InMemoryWorkerRegistry();
  private final WorkerRegistryService workerRegistryService =
      new WorkerRegistryService(new InMemoryWorkerStatusRepository(), workerRegistry);

  private final PauseInstanceExecutor executor =
      new PauseInstanceExecutor(
          instanceRegistryRepository,
          workerTaskManager,
          dispatcherTaskManager,
          workerRegistryService,
          INSTANCE_SCHEMA);

  @Test
  void shouldPauseInstance() {
    // given
    instanceRegistryRepository.addInstance(INSTANCE_SCHEMA, true);
    workerExists(1);
    workerExists(2);
    workerExists(3);
    dispatcherTaskExists();

    // when
    executor.execute(new Command("id", PAUSE_INSTANCE.name(), null, 1));

    // then
    assertThatDispatcherTaskIsSuspended();
    assertWorkerTaskIsSuspended(1);
    assertWorkerTaskIsSuspended(2);
    assertWorkerTaskIsSuspended(3);
    assertThat(instanceRegistryRepository.fetch(INSTANCE_SCHEMA).isActive()).isFalse();
  }

  private void workerExists(int workerId) {
    taskExists(ObjectName.from(INSTANCE_SCHEMA, ComponentNames.workerTask(new WorkerId(workerId))));
    workerRegistry.insertWorkers(1);
    workerRegistry.setWorkersStatus(WorkerLifecycleStatus.ACTIVE, List.of(new WorkerId(workerId)));
  }

  private void dispatcherTaskExists() {
    taskExists(ObjectName.from(INSTANCE_SCHEMA, DISPATCHER_TASK));
  }

  private void taskExists(ObjectName name) {
    taskRepository.create(
        new TaskDefinition(
            new TaskProperties.Builder(name, "def", "1 MINUTE").withState("started").build()),
        false,
        false);
  }

  private void assertThatDispatcherTaskIsSuspended() {
    assertThatTaskIsSuspended(ObjectName.from(INSTANCE_SCHEMA, DISPATCHER_TASK));
  }

  private void assertWorkerTaskIsSuspended(int workerId) {
    assertThatTaskIsSuspended(
        ObjectName.from(INSTANCE_SCHEMA, ComponentNames.workerTask(new WorkerId(workerId))));
  }

  private void assertThatTaskIsSuspended(ObjectName name) {
    assertThat(taskRepository.fetch(name).fetch()).isSuspended();
  }
}
