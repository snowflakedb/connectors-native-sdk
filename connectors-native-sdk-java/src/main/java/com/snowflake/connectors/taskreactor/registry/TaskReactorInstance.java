/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.registry;

import com.snowflake.connectors.common.object.Identifier;
import java.util.Objects;

/** Task Reactor instance representation. */
public class TaskReactorInstance {

  private final Identifier instanceName;
  private final boolean isInitialized;
  private final boolean isActive;

  /**
   * Creates a new {@link TaskReactorInstance}.
   *
   * @param instanceName Task Reactor instance name
   * @param isInitialized is instance initialized
   * @param isActive is instance active
   */
  public TaskReactorInstance(Identifier instanceName, boolean isInitialized, boolean isActive) {
    this.instanceName = instanceName;
    this.isInitialized = isInitialized;
    this.isActive = isActive;
  }

  /**
   * Returns the instance name.
   *
   * @return instance name
   */
  public Identifier instanceName() {
    return instanceName;
  }

  /**
   * Returns whether the instance is initialized.
   *
   * @return whether the instance is initialized
   */
  public boolean isInitialized() {
    return isInitialized;
  }

  /**
   * Returns whether the instance is active.
   *
   * @return whether the instance is active
   */
  public boolean isActive() {
    return isActive;
  }

  /**
   * Returns a copy of this instance, with the active parameter set to the specified value.
   *
   * @param isActive whether the instance is active
   * @return a copy of this instance
   */
  public TaskReactorInstance withActive(boolean isActive) {
    return new TaskReactorInstance(instanceName, isInitialized, isActive);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskReactorInstance that = (TaskReactorInstance) o;
    return isInitialized == that.isInitialized
        && isActive == that.isActive
        && Objects.equals(instanceName.toSqlString(), that.instanceName.toSqlString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(instanceName, isInitialized, isActive);
  }
}
