/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import static com.snowflake.connectors.util.sql.SqlStringFormatter.quoted;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;

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
    session.sql(String.format("EXECUTE TASK %s", identifier())).toLocalIterator();
  }

  @Override
  public boolean checkIfExists() {
    return session
            .sql(
                String.format(
                    "SHOW TASKS LIKE '%s' IN SCHEMA %s",
                    taskName.getName().toSqlString(), taskName.getSchema().toSqlString()))
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
    session.sql(String.format("DROP TASK %s", identifier())).toLocalIterator();
  }

  @Override
  public void dropIfExists() {
    session.sql(String.format("DROP TASK IF EXISTS %s", identifier())).toLocalIterator();
  }

  @Override
  public void alterSchedule(String schedule) {
    alterTask(String.format("SET SCHEDULE = '%s'", schedule));
  }

  @Override
  public void alterScheduleIfExists(String schedule) {
    alterTaskIfExists(String.format("SET SCHEDULE = '%s'", schedule));
  }

  @Override
  public void alterWarehouse(String warehouse) {
    alterTask(String.format("SET WAREHOUSE = %s", warehouse));
  }

  @Override
  public void alterWarehouseIfExists(String warehouse) {
    alterTaskIfExists(String.format("SET WAREHOUSE = %s", warehouse));
  }

  @Override
  public void grantMonitorPrivilegeToRole(String role) {
    session
        .sql(
            String.format(
                "GRANT MONITOR ON TASK %s TO APPLICATION ROLE %s", taskName.getEscapedName(), role))
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
            quoted("allow_overlapping_execution"))
        .first()
        .map(this::mapToProperties)
        .orElseThrow(() -> new TaskNotFoundException(taskName));
  }

  private TaskProperties mapToProperties(Row row) {
    return new TaskProperties.Builder(taskName, row.getString(0), row.getString(1))
        .withWarehouse(row.getString(2))
        .withState(row.getString(3))
        .withAllowOverlappingExecution(Boolean.parseBoolean(row.getString(4)))
        .build();
  }

  private String identifier() {
    return String.format("IDENTIFIER('%s')", taskName.getEscapedName());
  }

  private void alterTask(String command) {
    session.sql(String.format("ALTER TASK %s %s", identifier(), command)).toLocalIterator();
  }

  private void alterTaskIfExists(String command) {
    session
        .sql(String.format("ALTER TASK IF EXISTS %s %s", identifier(), command))
        .toLocalIterator();
  }
}
