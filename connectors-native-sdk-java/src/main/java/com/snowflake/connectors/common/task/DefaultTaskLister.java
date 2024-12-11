/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.quoted;

import com.snowflake.connectors.common.object.Identifier.AutoQuoting;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Default implementation of {@link TaskLister}. */
public class DefaultTaskLister implements TaskLister {

  private final Session session;

  /**
   * Creates a new {@link DefaultTaskLister}.
   *
   * @param session Snowpark session object
   */
  public DefaultTaskLister(Session session) {
    this.session = session;
  }

  @Override
  public Optional<TaskProperties> showTask(ObjectName taskName) {
    var schema = taskName.getSchema();
    if (schema.isEmpty()) {
      return Optional.empty();
    }

    var foundTasks = showTasks(schema.get().getValue(), taskName.getName().getUnquotedValue());
    return foundTasks.stream().findFirst();
  }

  @Override
  public List<TaskProperties> showTasks(String schema) {
    return showQueriedTasks("show tasks in schema " + schema);
  }

  @Override
  public List<TaskProperties> showTasks(String schema, String like) {
    return showQueriedTasks(
        String.format("show tasks like %s in schema %s", asVarchar(like), schema));
  }

  private List<TaskProperties> showQueriedTasks(String query) {
    return Arrays.stream(
            session
                .sql(query)
                .select(
                    quoted("schema_name"),
                    quoted("name"),
                    quoted("definition"),
                    quoted("schedule"),
                    quoted("state"),
                    quoted("warehouse"),
                    quoted("condition"),
                    quoted("allow_overlapping_execution"),
                    quoted("predecessors"))
                .collect())
        .map(this::mapToProperties)
        .sorted(Comparator.comparing(task -> task.name().getValue()))
        .collect(Collectors.toList());
  }

  private TaskProperties mapToProperties(Row row) {
    ObjectName name = ObjectName.from(row.getString(0), row.getString(1));

    return new TaskProperties.Builder(name, row.getString(2), row.getString(3))
        .withState(row.getString(4))
        .withWarehouse(row.getString(5), AutoQuoting.ENABLED)
        .withCondition(row.getString(6))
        .withAllowOverlappingExecution(Boolean.parseBoolean(row.getString(7)))
        .withPredecessors(mapVariantsToTaskRefs(row.getListOfVariant(8)))
        .build();
  }

  private List<TaskRef> mapVariantsToTaskRefs(List<Variant> variants) {
    return variants.stream()
        .map(Variant::asString)
        .map(ObjectName::fromString)
        .map(name -> TaskRef.of(session, name))
        .collect(Collectors.toList());
  }
}
