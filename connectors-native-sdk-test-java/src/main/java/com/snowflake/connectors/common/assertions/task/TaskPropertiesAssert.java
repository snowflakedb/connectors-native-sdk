/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.task;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskProperties;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link TaskProperties}. */
public class TaskPropertiesAssert extends AbstractAssert<TaskPropertiesAssert, TaskProperties> {

  public TaskPropertiesAssert(TaskProperties taskProperties, Class<?> selfType) {
    super(taskProperties, selfType);
  }

  /**
   * Asserts that these task properties have a task name equal to the specified value.
   *
   * @param name expected name
   * @return this assertion
   */
  public TaskPropertiesAssert hasObjectName(ObjectName name) {
    assertThat(actual.name()).isEqualTo(name);
    return this;
  }

  /**
   * Asserts that these task properties have a definition equal to the specified value.
   *
   * @param definition expected definition
   * @return this assertion
   */
  public TaskPropertiesAssert hasDefinition(String definition) {
    assertThat(actual.definition()).isEqualTo(definition);
    return this;
  }

  /**
   * Asserts that these task properties have a schedule equal to the specified value.
   *
   * @param schedule expected schedule
   * @return this assertion
   */
  public TaskPropertiesAssert hasSchedule(String schedule) {
    assertThat(actual.schedule()).isEqualTo(schedule);
    return this;
  }

  /**
   * Asserts that these task properties have a state equal to the specified value.
   *
   * @param state expected state
   * @return this assertion
   */
  public TaskPropertiesAssert hasState(String state) {
    assertThat(actual.state()).isEqualToIgnoringCase(state);
    return this;
  }

  /**
   * Asserts that these task properties have a state equal to {@code suspended}.
   *
   * @return this assertion
   */
  public TaskPropertiesAssert isSuspended() {
    assertThat(actual.state()).isEqualToIgnoringCase("suspended");
    return this;
  }

  /**
   * Asserts that these task properties have a state equal to {@code started}.
   *
   * @return this assertion
   */
  public TaskPropertiesAssert isStarted() {
    assertThat(actual.state()).isEqualToIgnoringCase("started");
    return this;
  }

  /**
   * Asserts that these task properties have a warehouse equal to the specified value.
   *
   * @param warehouse expected warehouse
   * @return this assertion
   */
  public TaskPropertiesAssert hasWarehouse(String warehouse) {
    assertThat(actual.warehouse()).isEqualTo(warehouse);
    return this;
  }

  /**
   * Asserts that these task properties have a condition equal to the specified value.
   *
   * @param condition expected condition
   * @return this assertion
   */
  public TaskPropertiesAssert hasCondition(String condition) {
    assertThat(actual.condition()).isEqualTo(condition);
    return this;
  }

  /**
   * Asserts that these task properties have an overlapping execution allowance equal to the
   * specified value.
   *
   * @param allow expected allowance
   * @return this assertion
   */
  public TaskPropertiesAssert hasAllowOverlappingExecution(boolean allow) {
    assertThat(actual.allowOverlappingExecution()).isEqualTo(allow);
    return this;
  }

  /**
   * Asserts that these task properties have a suspension after failures num equal to the specified
   * value.
   *
   * @param num expected failures num
   * @return this assertion
   */
  public TaskPropertiesAssert hasSuspendTaskAfterNumFailures(Integer num) {
    assertThat(actual.suspendTaskAfterNumFailures()).isEqualTo(num);
    return this;
  }
}
