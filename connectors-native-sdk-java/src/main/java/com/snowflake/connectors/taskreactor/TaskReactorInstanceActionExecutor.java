/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.snowpark_java.Session;
import java.util.function.Consumer;

/** Utility class for executing actions on all Task Reactor instances. */
public class TaskReactorInstanceActionExecutor {

  private final TaskReactorExistenceVerifier existenceVerifier;
  private final InstanceRegistryRepository instanceRegistryRepository;

  /**
   * Returns a new instance of the default executor implementation.
   *
   * @param session Snowpark session object
   * @return a new executor instance
   */
  public static TaskReactorInstanceActionExecutor getInstance(Session session) {
    return new TaskReactorInstanceActionExecutor(
        TaskReactorExistenceVerifier.getInstance(session),
        InstanceRegistryRepository.getInstance(session));
  }

  /**
   * Creates a new {@link TaskReactorInstanceActionExecutor}.
   *
   * @param existenceVerifier Task Reactor existence verifier
   * @param instanceRegistryRepository Task Reactor instance registry repository
   */
  public TaskReactorInstanceActionExecutor(
      TaskReactorExistenceVerifier existenceVerifier,
      InstanceRegistryRepository instanceRegistryRepository) {
    this.existenceVerifier = existenceVerifier;
    this.instanceRegistryRepository = instanceRegistryRepository;
  }

  /**
   * Executes the specified actions for all initialized Task Reactor instances.
   *
   * @param action action to be executed
   */
  public void applyToAllInitializedTaskReactorInstances(Consumer<Identifier> action) {
    if (!existenceVerifier.isTaskReactorConfigured()) {
      return;
    }

    instanceRegistryRepository
        .fetchAllInitialized()
        .forEach(instance -> action.accept(instance.instanceName()));
  }
}
