/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

/** Class representing additional task parameters. */
public class TaskParameters {

  private final Map<String, String> parameters;

  public TaskParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public Optional<String> getParameter(String parameter) {
    return Optional.ofNullable(parameters.get(parameter));
  }

  public boolean isNotEmpty() {
    return !parameters.isEmpty();
  }

  public Stream<Entry<String, String>> stream() {
    return parameters.entrySet().stream();
  }
}
