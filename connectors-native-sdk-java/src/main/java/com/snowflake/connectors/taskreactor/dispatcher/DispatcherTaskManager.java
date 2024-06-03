/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.dispatcher;

import static com.snowflake.connectors.taskreactor.ComponentNames.DISPATCHER_TASK;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.Session;

/** Component for managing dispatcher task. */
public class DispatcherTaskManager {

  private final Identifier instanceSchema;
  private final TaskRepository taskRepository;

  /**
   * Creates a new {@link DispatcherTaskManager}.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   * @return new manager instance
   */
  public static DispatcherTaskManager from(Session session, Identifier instanceSchema) {
    return new DispatcherTaskManager(instanceSchema, TaskRepository.getInstance(session));
  }

  DispatcherTaskManager(Identifier instanceSchema, TaskRepository taskRepository) {
    this.instanceSchema = instanceSchema;
    this.taskRepository = taskRepository;
  }

  /** Suspends dispatcher task */
  public void suspendDispatcherTask() {
    ObjectName dispatcherTask = ObjectName.from(instanceSchema, DISPATCHER_TASK);
    taskRepository.fetch(dispatcherTask).suspendIfExists();
  }

  /** Resumes dispatcher task */
  public void resumeDispatcherTask() {
    ObjectName dispatcherTask = ObjectName.from(instanceSchema, DISPATCHER_TASK);
    taskRepository.fetch(dispatcherTask).resume();
  }
}
