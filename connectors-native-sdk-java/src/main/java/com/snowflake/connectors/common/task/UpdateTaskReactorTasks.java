/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.registry.InstanceRegistryRepository;
import com.snowflake.snowpark_java.Session;

/** An interface providing update operations on tasks. */
public interface UpdateTaskReactorTasks {

  /**
   * Updates all worker instances with provided warehouse
   *
   * @param warehouse warehouse name
   */
  void update(Identifier warehouse);

  /**
   * Updates provided worker instance with provided warehouse
   *
   * @param instance instance name
   * @param warehouse warehouse name
   */
  void updateInstance(String instance, Identifier warehouse);

  /**
   * Returns a new instance of the default implementation.
   *
   * @param session Snowpark session object
   * @return a new {@link UpdateTaskReactorTasks} instance
   */
  static UpdateTaskReactorTasks getInstance(Session session) {
    return new DefaultUpdateTaskReactorTasks(
        InstanceRegistryRepository.getInstance(session),
        TaskReactorInstanceComponentProvider.getInstance(session),
        TaskRepository.getInstance(session));
  }
}
