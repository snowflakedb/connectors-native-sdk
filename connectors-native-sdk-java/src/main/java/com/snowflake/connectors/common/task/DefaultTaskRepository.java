/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static java.lang.String.format;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Default implementation of {@link TaskRepository}. */
public class DefaultTaskRepository implements TaskRepository {

  private final Session session;

  /**
   * Creates a new {@link DefaultTaskRepository}.
   *
   * @param session Snowpark session object
   */
  public DefaultTaskRepository(Session session) {
    this.session = session;
  }

  @Override
  public TaskRef create(TaskDefinition definition, boolean replace, boolean ifNotExists) {
    List<String> query = new ArrayList<>();
    String create = replace ? "create or replace" : "create";
    String exists = ifNotExists ? "if not exists" : "";

    query.add(
        format(
            "%s task %s identifier('%s')",
            create, exists, definition.properties().name().getValue()));
    query.add(
        format(
            "allow_overlapping_execution = %s",
            definition.properties().allowOverlappingExecution()));

    if (definition.properties().schedule() != null) {
      query.add(format("schedule = '%s'", definition.properties().schedule()));
    }

    if (definition.properties().warehouse() != null) {
      query.add(format("warehouse = %s", definition.properties().warehouse()));
    }

    if (definition.properties().suspendTaskAfterNumFailures() != null) {
      query.add(
          format(
              "SUSPEND_TASK_AFTER_NUM_FAILURES = %s",
              definition.properties().suspendTaskAfterNumFailures()));
    }

    if (definition.properties().userTaskTimeoutMs() != null) {
      query.add(
          String.format("USER_TASK_TIMEOUT_MS = %s", definition.properties().userTaskTimeoutMs()));
    }

    if (!definition.properties().predecessors().isEmpty()) {
      List<String> predecessorNames =
          definition.properties().predecessors().stream()
              .map(task -> task.name().getValue())
              .collect(Collectors.toList());
      query.add(format("after %s", String.join(",", predecessorNames)));
    }

    if (definition.properties().condition() != null) {
      query.add(format("when %s", definition.properties().condition()));
    }

    definition
        .parameters()
        .map(TaskParameters::stream)
        .orElse(Stream.empty())
        .map(entry -> format("%s = %s", entry.getKey(), entry.getValue()))
        .forEach(query::add);

    query.add(format("as %s", definition.properties().definition()));

    String result = executeQuery(query);
    validateResult(result, definition.properties().name());

    return DefaultTaskRef.of(session, definition.properties().name());
  }

  @Override
  public TaskRef fetch(ObjectName objectName) {
    return DefaultTaskRef.of(session, objectName);
  }

  private String executeQuery(List<String> query) {
    try {
      Row[] queryResult = session.sql(String.join("\n", query)).collect();
      return queryResult.length > 0
          ? queryResult[0].getString(0)
          : "Unknown exception, returned message was empty.";
    } catch (Exception exception) {
      throw new TaskCreationException(exception.getMessage());
    }
  }

  private static void validateResult(String result, ObjectName taskName) {
    if (!result.endsWith("successfully created.") && !result.endsWith("statement succeeded.")) {
      throw new TaskCreationException(
          format("Creation of task %s failed with message: %s", taskName.getValue(), result));
    }
  }
}
