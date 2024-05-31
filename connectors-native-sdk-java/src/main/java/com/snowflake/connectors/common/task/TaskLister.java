/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Session;
import java.util.List;
import java.util.Optional;

/** A simple utility for listing task properties. */
public interface TaskLister {

  /**
   * Fetches a task for a given object name.
   *
   * @param taskName object name of the task to fetch
   * @return properties of the fetched task
   */
  Optional<TaskProperties> showTask(ObjectName taskName);

  /**
   * Fetches all tasks which are present in given schema.
   *
   * @param schema structure to be queried to find all existing tasks
   * @return List of tasks in schema.
   */
  List<TaskProperties> showTasks(String schema);

  /**
   * Fetches tasks which are present in given schema and contains given string.
   *
   * @param schema structure to be queried to find all existing tasks
   * @param like string representing expression to compare to task names
   * @return List of tasks in schema containing like statement.
   */
  List<TaskProperties> showTasks(String schema, String like);

  /**
   * Returns a new instance of the default implementation for {@link TaskLister}.
   *
   * @param session Snowpark session object
   * @return {@link TaskLister} instance
   */
  static TaskLister getInstance(Session session) {
    return new DefaultTaskLister(session);
  }
}
