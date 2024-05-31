/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.snowpark_java.Session;

/** A repository providing general operations on tasks. */
public interface TaskRepository {

  /**
   * Creates or replaces task with given definition in the database.
   *
   * @param definition set of properties defining task to be created
   * @param replace flag to replace task if it existed
   * @param ifNotExists flag to create task only when it did not exist
   * @return Reference to the created task.
   * @throws TaskCreationException when any of the parameters/properties are incorrect.
   */
  TaskRef create(TaskDefinition definition, boolean replace, boolean ifNotExists);

  /**
   * Provides database reference to task without executing database call.
   *
   * @param objectName name of the referenced task
   * @return Reference task object.
   */
  TaskRef fetch(ObjectName objectName);

  /**
   * Returns a new instance of the default implementation for {@link TaskRepository}
   *
   * @param session Snowpark session object
   * @return {@link TaskRepository} instance
   */
  static TaskRepository getInstance(Session session) {
    return new DefaultTaskRepository(session);
  }
}
