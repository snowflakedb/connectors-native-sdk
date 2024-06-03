/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import java.util.Optional;

/**
 * Class representing a <a href="https://docs.snowflake.com/en/sql-reference/sql/create-task">
 * Snowflake task creation definition</a>.
 */
public class TaskDefinition {

  private final TaskProperties properties;
  private final TaskParameters parameters;

  /**
   * Creates a new {@link TaskDefinition}.
   *
   * @param properties task properties
   */
  public TaskDefinition(TaskProperties properties) {
    this(properties, null);
  }

  /**
   * Creates a new {@link TaskDefinition}.
   *
   * @param properties task properties
   * @param parameters additional task parameters
   */
  public TaskDefinition(TaskProperties properties, TaskParameters parameters) {
    this.properties = properties;
    this.parameters = parameters;
  }

  /**
   * Returns the task properties.
   *
   * @return task properties
   */
  public TaskProperties properties() {
    return properties;
  }

  /**
   * Returns the additional task parameters.
   *
   * @return additional task parameters
   */
  public Optional<TaskParameters> parameters() {
    return Optional.ofNullable(parameters);
  }
}
