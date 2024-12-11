/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.quoted;
import static java.lang.String.format;

import com.snowflake.connectors.common.object.Identifier.AutoQuoting;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Default implementation of {@link TaskRef}. */
public class DefaultTaskRef implements TaskRef {

  private final Session session;
  private final ObjectName taskName;

  private DefaultTaskRef(Session session, ObjectName taskName) {
    this.session = session;
    this.taskName = taskName;
  }

  static DefaultTaskRef of(Session session, ObjectName taskName) {
    return new DefaultTaskRef(session, taskName);
  }

  @Override
  public ObjectName name() {
    return taskName;
  }

  @Override
  public void execute() {
    session.sql(format("EXECUTE TASK %s", identifier())).toLocalIterator();
  }

  @Override
  public boolean checkIfExists() {
    var schema = taskName.getSchema();
    if (schema.isEmpty()) {
      return false;
    }

    return session
            .sql(
                format(
                    "SHOW TASKS LIKE %s IN SCHEMA %s",
                    asVarchar(taskName.getName().getUnquotedValue()), schema.get().getValue()))
            .collect()
            .length
        == 1;
  }

  @Override
  public void resume() {
    alterTask("RESUME");
  }

  @Override
  public void resumeIfExists() {
    alterTaskIfExists("RESUME");
  }

  @Override
  public void suspend() {
    alterTask("SUSPEND");
  }

  @Override
  public void suspendIfExists() {
    alterTaskIfExists("SUSPEND");
  }

  @Override
  public void drop() {
    session.sql(format("DROP TASK %s", identifier())).toLocalIterator();
  }

  @Override
  public void dropIfExists() {
    session.sql(format("DROP TASK IF EXISTS %s", identifier())).toLocalIterator();
  }

  @Override
  public void alterSchedule(String schedule) {
    alterTask(format("SET SCHEDULE = %s", asVarchar(schedule)));
  }

  @Override
  public void alterScheduleIfExists(String schedule) {
    alterTaskIfExists(format("SET SCHEDULE = %s", asVarchar(schedule)));
  }

  @Override
  public void alterWarehouse(String warehouse) {
    alterTask(format("SET WAREHOUSE = %s", asVarchar(warehouse)));
  }

  @Override
  public void alterWarehouseIfExists(String warehouse) {
    alterTaskIfExists(format("SET WAREHOUSE = %s", asVarchar(warehouse)));
  }

  @Override
  public void grantMonitorPrivilegeToRole(String role) {
    session
        .sql(format("GRANT MONITOR ON TASK %s TO APPLICATION ROLE %s", taskName.getValue(), role))
        .toLocalIterator();
  }

  @Override
  public TaskProperties fetch() {
    return session
        .sql(String.format("DESCRIBE TASK %s", identifier()))
        .select(
            quoted("definition"),
            quoted("schedule"),
            quoted("warehouse"),
            quoted("state"),
            quoted("allow_overlapping_execution"),
            quoted("predecessors"))
        .first()
        .map(this::mapToProperties)
        .orElseThrow(() -> new TaskNotFoundException(taskName));
  }

  private TaskProperties mapToProperties(Row row) {
    return new TaskProperties.Builder(taskName, row.getString(0), row.getString(1))
        .withWarehouse(row.getString(2), AutoQuoting.ENABLED)
        .withState(row.getString(3))
        .withAllowOverlappingExecution(Boolean.parseBoolean(row.getString(4)))
        .withPredecessors(mapVariantsToTaskRefs(row.getListOfVariant(5)))
        .build();
  }

  private List<TaskRef> mapVariantsToTaskRefs(List<Variant> variants) {
    return variants.stream()
        .map(Variant::asString)
        .map(ObjectName::fromString)
        .map(name -> TaskRef.of(session, name))
        .collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultTaskRef that = (DefaultTaskRef) o;
    return Objects.equals(taskName, that.taskName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(taskName);
  }

  private String identifier() {
    return format("IDENTIFIER(%s)", asVarchar(taskName.getValue()));
  }

  private void alterTask(String command) {
    session.sql(format("ALTER TASK %s %s", identifier(), command)).toLocalIterator();
  }

  private void alterTaskIfExists(String command) {
    session.sql(format("ALTER TASK IF EXISTS %s %s", identifier(), command)).toLocalIterator();
  }
}
