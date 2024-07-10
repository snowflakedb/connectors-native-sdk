/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.List;
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
        String.format(
            "%s task %s identifier('%s')",
            create, exists, definition.properties().name().getValue()));
    query.add(String.format("schedule = '%s'", definition.properties().schedule()));
    query.add(
        String.format(
            "allow_overlapping_execution = %s",
            definition.properties().allowOverlappingExecution()));

    if (definition.properties().warehouse() != null) {
      query.add(String.format("warehouse = %s", definition.properties().warehouse()));
    }
    if (definition.properties().suspendTaskAfterNumFailures() != null) {
      query.add(
          String.format(
              "SUSPEND_TASK_AFTER_NUM_FAILURES = %s",
              definition.properties().suspendTaskAfterNumFailures()));
    }
    if (definition.properties().condition() != null) {
      query.add(String.format("when %s", definition.properties().condition()));
    }

    definition
        .parameters()
        .map(TaskParameters::stream)
        .orElse(Stream.empty())
        .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
        .forEach(query::add);

    query.add(String.format("as %s", definition.properties().definition()));

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
      return session
          .sql(String.join("\n", query))
          .first()
          .map(row -> row.getString(0))
          .orElse("Unknown exception, returned message was empty.");

    } catch (Exception exception) {
      throw new TaskCreationException(exception.getMessage());
    }
  }

  private static void validateResult(String result, ObjectName taskName) {
    if (!result.endsWith("successfully created.")) {
      throw new TaskCreationException(
          String.format(
              "Creation of task %s failed with message: %s", taskName.getValue(), result));
    }
  }
}
