/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.lifecycle;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.InMemoryConfiguredTaskReactorExistenceVerifier;
import com.snowflake.connectors.taskreactor.InMemoryTaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.TaskReactorExistenceVerifier;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import org.junit.jupiter.api.Test;

public class PauseTaskReactorServiceTest {

  InMemoryTaskReactorInstanceComponentProvider componentProvider =
      new InMemoryTaskReactorInstanceComponentProvider();
  TaskReactorExistenceVerifier existenceVerifier =
      new InMemoryConfiguredTaskReactorExistenceVerifier();
  InMemoryInstanceRegistryRepository instanceRegistryRepository =
      new InMemoryInstanceRegistryRepository();
  PauseTaskReactorService pauseTaskReactorService =
      new PauseTaskReactorService(
          componentProvider,
          new TaskReactorInstanceActionExecutor(existenceVerifier, instanceRegistryRepository));

  @Test
  void shouldPauseTaskReactorInstance() {
    // given
    Identifier instance = Identifier.from("i1");

    // when
    pauseTaskReactorService.pauseInstance(instance);

    // then
    assertPauseCommandWasAddedToCommandQueue(instance);
  }

  @Test
  void shouldPauseAllTaskReactorInstances() {
    // given
    Identifier instance1 = Identifier.from("xD");
    Identifier instance2 = Identifier.from("xDD");
    Identifier instance3 = Identifier.from("xDDD");
    instanceRegistryRepository.addInstance(instance1, true);
    instanceRegistryRepository.addInstance(instance2, true);
    instanceRegistryRepository.addInstance(instance3, true);

    // when
    pauseTaskReactorService.pauseAllInstances();

    // then
    assertPauseCommandWasAddedToCommandQueue(instance1);
    assertPauseCommandWasAddedToCommandQueue(instance2);
    assertPauseCommandWasAddedToCommandQueue(instance3);
  }

  private void assertPauseCommandWasAddedToCommandQueue(Identifier instance) {
    CommandsQueueRepository commandsQueueRepository =
        componentProvider.commandsQueueRepository(instance);
    assertThat(commandsQueueRepository.fetchAllSupportedOrderedBySeqNo())
        .hasSize(1)
        .satisfiesOnlyOnce(command -> assertThat(command.getType()).isEqualTo(PAUSE_INSTANCE));
  }
}
