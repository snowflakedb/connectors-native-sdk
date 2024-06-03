/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.Reference;
import java.util.Objects;
import java.util.Optional;

/** Class representing main task properties. */
public class TaskProperties {

  private final ObjectName objectName;
  private final String definition;
  private final String schedule;
  private final String state;
  private final Identifier warehouseIdentifier;
  private final Reference warehouseReference;
  private final String condition;
  private final boolean allowOverlappingExecution;
  private final Integer suspendTaskAfterNumFailures;

  private TaskProperties(
      ObjectName objectName,
      String definition,
      String schedule,
      String state,
      Reference warehouseReference,
      Identifier warehouseIdentifier,
      String condition,
      boolean allowOverlappingExecution,
      Integer suspendTaskAfterNumFailures) {
    this.objectName = objectName;
    this.definition = definition;
    this.schedule = schedule;
    this.state = state;
    this.warehouseReference = warehouseReference;
    this.warehouseIdentifier = warehouseIdentifier;
    this.condition = condition;
    this.allowOverlappingExecution = allowOverlappingExecution;
    this.suspendTaskAfterNumFailures = suspendTaskAfterNumFailures;
  }

  /** Builder for the {@link TaskProperties}. */
  public static class Builder {

    private final ObjectName objectName;
    private final String definition;
    private String schedule;
    private String state;
    private Reference warehouseReference;
    private Identifier warehouseIdentifier;
    private String condition;
    private boolean allowOverlappingExecution;
    private Integer suspendTaskAfterNumFailures;

    /**
     * Creates a new {@link Builder}.
     *
     * @param objectName task object name
     * @param definition task definition
     * @param schedule task schedule
     */
    public Builder(ObjectName objectName, String definition, String schedule) {
      this.objectName = objectName;
      this.definition = definition;
      this.schedule = schedule;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @param properties task properties
     */
    public Builder(TaskProperties properties) {
      this.objectName = properties.objectName;
      this.definition = properties.definition;
      this.schedule = properties.schedule;
      this.state = properties.state;
      this.warehouseReference = properties.warehouseReference;
      this.warehouseIdentifier = properties.warehouseIdentifier;
      this.condition = properties.condition;
      this.allowOverlappingExecution = properties.allowOverlappingExecution;
      this.suspendTaskAfterNumFailures = properties.suspendTaskAfterNumFailures;
    }

    /**
     * Sets the task state used to build task properties.
     *
     * @param state task state
     * @return this builder
     */
    public Builder withState(String state) {
      this.state = state;
      return this;
    }

    /**
     * Sets the warehouse reference used to build task properties.
     *
     * @param warehouseReference warehouse reference
     * @return this builder
     */
    public Builder withWarehouse(Reference warehouseReference) {
      this.warehouseReference = warehouseReference;
      this.warehouseIdentifier = null;
      return this;
    }

    /**
     * Sets the warehouse identifier used to build task properties.
     *
     * @param warehouseIdentifier warehouse identifier
     * @return this builder
     */
    public Builder withWarehouse(Identifier warehouseIdentifier) {
      this.warehouseIdentifier = warehouseIdentifier;
      this.warehouseReference = null;
      return this;
    }

    /**
     * Sets the warehouse used to build task properties.
     *
     * @param warehouse warehouse
     * @return this builder
     */
    public Builder withWarehouse(String warehouse) {
      if (Reference.validate(warehouse)) {
        return withWarehouse(Reference.of(warehouse));
      }
      return withWarehouse(Identifier.fromWithAutoQuoting(warehouse));
    }

    /**
     * Sets the task condition used to build task properties.
     *
     * @param condition task condition
     * @return this builder
     */
    public Builder withCondition(String condition) {
      this.condition = condition;
      return this;
    }

    /**
     * Sets the task overlapping execution allowance used to build task properties.
     *
     * @param allowOverlappingExecution overlapping execution allowance
     * @return this builder
     */
    public Builder withAllowOverlappingExecution(boolean allowOverlappingExecution) {
      this.allowOverlappingExecution = allowOverlappingExecution;
      return this;
    }

    /**
     * Sets the number of task failures before suspension used to build task properties.
     *
     * @param failuresNumberToSuspension number of task failures before suspension
     * @return this builder
     */
    public Builder withSuspendTaskAfterNumFailures(int failuresNumberToSuspension) {
      this.suspendTaskAfterNumFailures = failuresNumberToSuspension;
      return this;
    }

    /**
     * Sets the task schedule used to build task properties.
     *
     * @param schedule task schedule
     * @return this builder
     */
    public Builder withSchedule(String schedule) {
      this.schedule = schedule;
      return this;
    }

    /**
     * Build new task properties.
     *
     * @return new task properties
     */
    public TaskProperties build() {
      return new TaskProperties(
          objectName,
          definition,
          schedule,
          state,
          warehouseReference,
          warehouseIdentifier,
          condition,
          allowOverlappingExecution,
          suspendTaskAfterNumFailures);
    }
  }

  /**
   * Returns the task object name.
   *
   * @return task object name
   */
  public ObjectName name() {
    return objectName;
  }

  /**
   * Returns the task definition.
   *
   * @return task definition
   */
  public String definition() {
    return definition;
  }

  /**
   * Returns the task schedule.
   *
   * @return task schedule
   */
  public String schedule() {
    return schedule;
  }

  /**
   * Returns the task state.
   *
   * @return task state
   */
  public String state() {
    return state;
  }

  /**
   * Returns the task warehouse.
   *
   * @return task warehouse
   */
  public String warehouse() {
    if (warehouseReference != null) {
      return warehouseReference.value();
    } else if (warehouseIdentifier != null) {
      return warehouseIdentifier.toSqlString();
    }

    return null;
  }

  /**
   * Returns the task warehouse reference.
   *
   * @return task warehouse reference
   */
  public Optional<Reference> warehouseReference() {
    return Optional.ofNullable(this.warehouseReference);
  }

  /**
   * Returns the task warehouse identifier.
   *
   * @return task warehouse identifier
   */
  public Optional<Identifier> warehouseIdentifier() {
    return Optional.ofNullable(this.warehouseIdentifier);
  }

  /**
   * Returns the task condition.
   *
   * @return task condition
   */
  public String condition() {
    return condition;
  }

  /**
   * Returns whether the task allows overlapping execution.
   *
   * @return whether the task allows overlapping execution
   */
  public boolean allowOverlappingExecution() {
    return allowOverlappingExecution;
  }

  /**
   * Returns the number of task failures before suspension.
   *
   * @return number of task failures before suspension
   */
  public Integer suspendTaskAfterNumFailures() {
    return suspendTaskAfterNumFailures;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskProperties that = (TaskProperties) o;
    return allowOverlappingExecution == that.allowOverlappingExecution
        && Objects.equals(objectName, that.objectName)
        && Objects.equals(definition, that.definition)
        && Objects.equals(schedule, that.schedule)
        && Objects.equals(state, that.state)
        && Objects.equals(warehouseIdentifier, that.warehouseIdentifier)
        && Objects.equals(warehouseReference, that.warehouseReference)
        && Objects.equals(condition, that.condition)
        && Objects.equals(suspendTaskAfterNumFailures, that.suspendTaskAfterNumFailures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        objectName,
        definition,
        schedule,
        state,
        warehouseIdentifier,
        warehouseReference,
        condition,
        allowOverlappingExecution,
        suspendTaskAfterNumFailures);
  }
}
