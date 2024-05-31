/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Session;

/** An interface that allows user to interact with Snowflake tasks. */
public interface TaskRef {

  /**
   * @return name of the referenced task
   */
  ObjectName name();

  /**
   * Executes immediately the referenced task.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   */
  void execute();

  /**
   * @return true if task can be matched with a task existing in Snowflake database, otherwise false
   */
  boolean checkIfExists();

  /**
   * Resumes suspended referenced task.
   *
   * <p>If the task is already resumed - the operation is ignored.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   */
  void resume();

  /**
   * Resumes suspended referenced task if it exists.
   *
   * <p>If the task is already resumed - the operation is ignored.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} not
   * authorized.
   */
  void resumeIfExists();

  /**
   * Suspends running referenced task.
   *
   * <p>If the task is already suspended - the operation is ignored.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   */
  void suspend();

  /**
   * Suspends running referenced task if it exists.
   *
   * <p>If the task is already suspended - the operation is ignored.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if not
   * authorized.
   */
  void suspendIfExists();

  /**
   * Removes referenced task from the database.
   *
   * <p>Current execution of the task is finished before the task is dropped.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   */
  void drop();

  /**
   * Removes referenced task from the database if it exists.
   *
   * <p>Current execution of the task is finished before the task is dropped.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   */
  void dropIfExists();

  /**
   * Changes schedule of referenced task.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist, not authorized, or schedule is invalid.
   *
   * @param schedule interval or cron expression which will be set
   */
  void alterSchedule(String schedule);

  /**
   * Changes schedule of referenced task if it exists.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist, not authorized, or schedule is invalid.
   *
   * @param schedule interval or cron expression which will be set
   */
  void alterScheduleIfExists(String schedule);

  /**
   * Changes warehouse of referenced task.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist, not authorized, or warehouse name is invalid.
   *
   * @param warehouse warehouse name which will be set
   */
  void alterWarehouse(String warehouse);

  /**
   * Changes warehouse of referenced task if it exists.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist, not authorized, or warehouse name is invalid.
   *
   * @param warehouse warehouse name which will be set
   */
  void alterWarehouseIfExists(String warehouse);

  /**
   * Grants monitor privilege to given application role.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist, not authorized, or role is invalid.
   *
   * @param role role which will be granted monitor privilege
   */
  void grantMonitorPrivilegeToRole(String role);

  /**
   * Fetches properties containing base information about the referenced task.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if task
   * does not exist or not authorized.
   *
   * @return properties of the referenced task
   */
  TaskProperties fetch();

  /**
   * Returns a new instance of the default implementation for {@link TaskRef}.
   *
   * @param session Snowpark session object
   * @param objectName Task reference name
   * @return {@link TaskRef} instance
   */
  static TaskRef of(Session session, ObjectName objectName) {
    return DefaultTaskRef.of(session, objectName);
  }
}
