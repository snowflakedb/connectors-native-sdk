/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

/** Class representing additional task parameters. */
public class TaskParameters {

  private final Map<String, String> parameters;

  /**
   * Creates a new {@link TaskParameters} instance.
   *
   * @param parameters task parameters
   */
  public TaskParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * Returns the value of the specified task parameter.
   *
   * @param parameter task parameter name
   * @return value of the specified task parameter
   */
  public Optional<String> getParameter(String parameter) {
    return Optional.ofNullable(parameters.get(parameter));
  }

  /**
   * Returns whether there are any additional task parameters.
   *
   * @return whether there are any additional task parameters
   */
  public boolean isNotEmpty() {
    return !parameters.isEmpty();
  }

  /**
   * Returns a stream iterating over entries of the parameters map.
   *
   * @return stream iterating over entries of the parameters map
   */
  public Stream<Entry<String, String>> stream() {
    return parameters.entrySet().stream();
  }
}
