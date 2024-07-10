/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.registry.InMemoryInstanceRegistryRepository;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TaskReactorInstanceActionExecutorTest {

  private static final Identifier INSTANCE_1 = Identifier.from("NeverGonnaGiveYouUp");
  private static final Identifier INSTANCE_2 = Identifier.from("NeverGonnaLetYouDown");
  private static final Identifier INSTANCE_3 = Identifier.from("NeverGonnaRunAroundAndDesertYou");
  private static final Identifier INSTANCE_4 = Identifier.from("NeverGonnaMakeYouCry");

  @Test
  void shouldExecuteActionForAllInitializedInstances() {
    // given
    var instanceRegistry = new InMemoryInstanceRegistryRepository();
    var existenceVerifier = mock(TaskReactorExistenceVerifier.class);
    when(existenceVerifier.isTaskReactorConfigured()).thenReturn(true);
    var executor = new TaskReactorInstanceActionExecutor(existenceVerifier, instanceRegistry);

    instanceRegistry.addInstance(INSTANCE_1, true, true);
    instanceRegistry.addInstance(INSTANCE_2, true, true);
    instanceRegistry.addInstance(INSTANCE_3, true, false);
    instanceRegistry.addInstance(INSTANCE_4, false, false);

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllInitializedTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).containsExactlyInAnyOrder(INSTANCE_1, INSTANCE_2, INSTANCE_3);
  }

  @Test
  void shouldDoNothingWhenTaskReactorIsNotConfigured() {
    // given
    var existenceVerifier = mock(TaskReactorExistenceVerifier.class);
    when(existenceVerifier.isTaskReactorConfigured()).thenReturn(false);
    var executor =
        new TaskReactorInstanceActionExecutor(
            existenceVerifier, new InMemoryInstanceRegistryRepository());

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllInitializedTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).isEmpty();
  }

  @Test
  void shouldDoNothingWhenTaskReactorIsConfiguredButThereAreNoInstances() {
    // given
    var existenceVerifier = mock(TaskReactorExistenceVerifier.class);
    when(existenceVerifier.isTaskReactorConfigured()).thenReturn(true);
    var executor =
        new TaskReactorInstanceActionExecutor(
            existenceVerifier, new InMemoryInstanceRegistryRepository());

    List<Identifier> invokedActions = new ArrayList<>();

    // when
    executor.applyToAllInitializedTaskReactorInstances(invokedActions::add);

    // then
    assertThat(invokedActions).isEmpty();
  }
}
