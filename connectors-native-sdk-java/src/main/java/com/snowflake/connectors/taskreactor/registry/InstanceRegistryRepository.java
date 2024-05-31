/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.registry;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Repository for managing TASK_REACTOR.INSTANCE_REGISTRY table */
public interface InstanceRegistryRepository {

  /**
   * Fetches all Task Reactor instances registered in the connector
   *
   * @return list of instances
   */
  List<TaskReactorInstance> fetchAll();

  /**
   * Fetches a Task Reactor instance with given identifier
   *
   * @param instanceId id of an instance
   * @return instance object when an instance with given id exists, otherwise null
   */
  TaskReactorInstance fetch(Identifier instanceId);

  /**
   * Changes isActive flag to false for given instance
   *
   * @param instanceId id of an instance
   */
  void setInactive(Identifier instanceId);

  /**
   * Changes isActive flag to true for given instance
   *
   * @param instanceId id of an instance
   */
  void setActive(Identifier instanceId);

  /**
   * Creates a new instance of {@link InstanceRegistryRepository}
   *
   * @param session session
   * @return new instance
   */
  static InstanceRegistryRepository getInstance(Session session) {
    return new DefaultInstanceRegistryRepository(session);
  }
}
