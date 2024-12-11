/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.lifecycle;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.taskreactor.ComponentNames.DISPATCHER_TASK;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.InMemoryTaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.TaskReactorExistenceVerifier;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResumeTaskReactorServiceTest {

  private InMemoryTaskReactorInstanceComponentProvider componentProvider;
  private InMemoryInstanceRegistryRepository instanceRegistryRepository;
  private TaskRepository taskRepository;
  private ResumeTaskReactorService resumeTaskReactorService;

  @BeforeEach
  public void setUp() {
    var existenceVerifier = mock(TaskReactorExistenceVerifier.class);
    when(existenceVerifier.isTaskReactorConfigured()).thenReturn(true);
    componentProvider = new InMemoryTaskReactorInstanceComponentProvider();
    instanceRegistryRepository = new InMemoryInstanceRegistryRepository();
    taskRepository = componentProvider.taskRepository();
    resumeTaskReactorService =
        new ResumeTaskReactorService(
            componentProvider,
            new TaskReactorInstanceActionExecutor(existenceVerifier, instanceRegistryRepository));
  }

  @Test
  void shouldResumeTaskReactorInstance() {
    // given
    Identifier instance = Identifier.from("i1");
    suspendedDispatcherTaskExists(instance);

    // when
    resumeTaskReactorService.resumeInstance(instance);

    // then
    assertResumeCommandWasAddedToCommandQueue(instance);
    assertDispatcherTaskIsResumed(instance);
  }

  @Test
  void shouldResumeAllTaskReactorInstances() {
    // given
    Identifier instance1 = Identifier.from("i1");
    Identifier instance2 = Identifier.from("i2");
    Identifier instance3 = Identifier.from("i3");
    instanceRegistryRepository.addInstance(instance1, true);
    instanceRegistryRepository.addInstance(instance2, true);
    instanceRegistryRepository.addInstance(instance3, true);
    suspendedDispatcherTaskExists(instance1);
    suspendedDispatcherTaskExists(instance2);
    suspendedDispatcherTaskExists(instance3);

    // when
    resumeTaskReactorService.resumeAllInstances();

    // then
    assertResumeCommandWasAddedToCommandQueue(instance1);
    assertResumeCommandWasAddedToCommandQueue(instance2);
    assertResumeCommandWasAddedToCommandQueue(instance3);
    assertDispatcherTaskIsResumed(instance1);
    assertDispatcherTaskIsResumed(instance2);
    assertDispatcherTaskIsResumed(instance3);
  }

  private void assertResumeCommandWasAddedToCommandQueue(Identifier instance) {
    CommandsQueue commandsQueue = componentProvider.commandsQueue(instance);
    assertThat(commandsQueue.fetchAllSupportedOrderedBySeqNo())
        .hasSize(1)
        .satisfiesOnlyOnce(command -> assertThat(command.getType()).isEqualTo(RESUME_INSTANCE));
  }

  private void suspendedDispatcherTaskExists(Identifier instance) {
    ObjectName dispatcherTask = ObjectName.from(instance, DISPATCHER_TASK);
    taskRepository.create(
        new TaskDefinition(
            new TaskProperties.Builder(dispatcherTask, "def", "1 MINUTE")
                .withState("suspended")
                .build()),
        false,
        false);
  }

  private void assertDispatcherTaskIsResumed(Identifier instance) {
    ObjectName dispatcherTask = ObjectName.from(instance, DISPATCHER_TASK);
    assertThat(taskRepository.fetch(dispatcherTask).fetch()).isStarted();
  }
}
