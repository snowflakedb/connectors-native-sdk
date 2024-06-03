/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.registry;

import com.snowflake.connectors.common.object.Identifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In memory implementation of {@link InstanceRegistryRepository}. */
public class InMemoryInstanceRegistryRepository implements InstanceRegistryRepository {

  private final Map<Identifier, TaskReactorInstance> store = new HashMap<>();

  @Override
  public List<TaskReactorInstance> fetchAll() {
    return new ArrayList<>(store.values());
  }

  @Override
  public TaskReactorInstance fetch(Identifier instance) {
    return store.get(instance);
  }

  @Override
  public void setInactive(Identifier instance) {
    store.computeIfPresent(instance, (identifier, value) -> value.withActive(false));
  }

  @Override
  public void setActive(Identifier instance) {
    store.computeIfPresent(instance, (identifier, value) -> value.withActive(true));
  }

  /**
   * Adds a new Task Reactor instance.
   *
   * @param instanceName instance name
   * @param isActive is the instance active
   */
  public void addInstance(Identifier instanceName, boolean isActive) {
    store.put(instanceName, new TaskReactorInstance(instanceName, true, isActive));
  }

  /** Clears this repository. */
  public void clear() {
    store.clear();
  }
}
