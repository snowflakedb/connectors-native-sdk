/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.snowpark_java.Session;

/**
 * Class which is used to provide components that require Task Reactor instance name. It is designed
 * to be used in features that have to execute some operations for all instances of Task Reactor
 * defined in a Connector.
 *
 * <p>DO NOT use it in classes where an operation needs to be executed only for a single instance
 * and that instance name is known.
 */
public interface TaskReactorInstanceComponentProvider {

  /**
   * Creates a new instance of {@link CommandsQueue} for a given Task Reactor instance
   *
   * @param instanceSchema name of Task Reactor instance
   * @return new instance of {@link CommandsQueue}
   */
  CommandsQueue commandsQueue(Identifier instanceSchema);

  /**
   * Creates a new instance of {@link DispatcherTaskManager} for a given Task Reactor instance
   *
   * @param instanceSchema name of Task Reactor instance
   * @return new instance of {@link DispatcherTaskManager}
   */
  DispatcherTaskManager dispatcherTaskManager(Identifier instanceSchema);

  /**
   * Creates a new instance of {@link TaskReactorInstanceComponentProvider}
   *
   * @param session Snowpark session
   * @return default implementation of {@link TaskReactorInstanceComponentProvider}
   */
  static TaskReactorInstanceComponentProvider getInstance(Session session) {
    return new DefaultTaskReactorInstanceComponentProvider(session);
  }
}
