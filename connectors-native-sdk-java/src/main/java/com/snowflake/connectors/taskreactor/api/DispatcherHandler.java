/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.Dispatcher;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler for the Task Reactor dispatcher. */
public class DispatcherHandler {

  private static final Logger log = LoggerFactory.getLogger(DispatcherHandler.class);

  /**
   * Default handler method for the {@code TASK_REACTOR.DISPATCHER} procedure.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance name
   * @return message about number of queue items consumed
   */
  public static String dispatchWorkItems(Session session, String instanceSchema) {
    log.debug("Starting dispatcher run for instance {}.", instanceSchema);
    return Dispatcher.from(session, Identifier.fromWithAutoQuoting(instanceSchema)).execute();
  }
}
