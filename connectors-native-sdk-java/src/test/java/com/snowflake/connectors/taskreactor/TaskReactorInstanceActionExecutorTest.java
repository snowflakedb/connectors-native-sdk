/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TaskReactorInstanceActionExecutorTest {

  private static final Identifier INSTANCE_1 = Identifier.from("NeverGonnaGiveYouUp");
  private static final Identifier INSTANCE_2 = Identifier.from("NeverGonnaLetYouDown");
  private static final Identifier INSTANCE_3 = Identifier.from("NeverGonnaRunAroundAndDesertYou");

  @Test
  void shouldExecuteActionForAllConfiguredInstances() {
    // given
    var instanceRegistry = new InMemoryInstanceRegistryRepository();
    var executor =
        new TaskReactorInstanceActionExecutor(
            new InMemoryConfiguredTaskReactorExistenceVerifier(), instanceRegistry);

    instanceRegistry.addInstance(INSTANCE_1, true);
    instanceRegistry.addInstance(INSTANCE_2, true);
    instanceRegistry.addInstance(INSTANCE_3, false);

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllExistingTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).containsExactlyInAnyOrder(INSTANCE_1, INSTANCE_2, INSTANCE_3);
  }

  @Test
  void shouldDoNothingWhenTaskReactorIsNotConfigured() {
    // given
    var executor =
        new TaskReactorInstanceActionExecutor(
            new InMemoryNotConfiguredTaskReactorExistenceVerifier(),
            new InMemoryInstanceRegistryRepository());

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllExistingTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).isEmpty();
  }

  @Test
  void shouldDoNothingWhenTaskReactorIsConfiguredButThereAreNoInstances() {
    // given
    var executor =
        new TaskReactorInstanceActionExecutor(
            new InMemoryConfiguredTaskReactorExistenceVerifier(),
            new InMemoryInstanceRegistryRepository());

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllExistingTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).isEmpty();
  }
}
