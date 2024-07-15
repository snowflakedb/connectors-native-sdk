/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.Dispatcher;
import com.snowflake.snowpark_java.Session;

/** Handler for the Task Reactor dispatcher. */
public class DispatcherHandler {

  /**
   * Default handler method for the {@code TASK_REACTOR.DISPATCHER} procedure.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance name
   * @return message about number of queue items consumed
   */
  public static String dispatchWorkItems(Session session, String instanceSchema) {
    return Dispatcher.from(session, Identifier.from(instanceSchema)).execute();
  }
}
