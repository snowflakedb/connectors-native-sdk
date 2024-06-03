/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link TaskReactorInstanceComponentProvider} */
class DefaultTaskReactorInstanceComponentProvider implements TaskReactorInstanceComponentProvider {

  private final Session session;

  DefaultTaskReactorInstanceComponentProvider(Session session) {
    this.session = session;
  }

  @Override
  public CommandsQueueRepository commandsQueueRepository(Identifier instanceSchema) {
    return CommandsQueueRepository.getInstance(session, instanceSchema);
  }

  @Override
  public DispatcherTaskManager dispatcherTaskManager(Identifier instanceSchema) {
    return DispatcherTaskManager.from(session, instanceSchema);
  }
}
