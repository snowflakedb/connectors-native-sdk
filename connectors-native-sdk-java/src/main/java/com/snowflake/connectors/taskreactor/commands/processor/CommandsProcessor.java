/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.snowpark_java.Session;

/**
 * Task Reactor Commands Processor mechanism that processes commands added to the commands queue.
 */
public interface CommandsProcessor {

  /** Processes commands from the {@code COMMANDS_QUEUE} table. */
  void processCommands();

  /**
   * Returns a new instance of the {@link DefaultCommandsProcessor} that is used by the Task
   * Reactor.
   *
   * @param session Snowpark session object
   * @param instanceName task reactor instance schema name
   * @return default, used by the task reactor implementation of the {@link CommandsProcessor}
   */
  static DefaultCommandsProcessor getInstance(Session session, Identifier instanceName) {
    return new DefaultCommandsProcessor(
        CommandsQueueRepository.getInstance(session, instanceName),
        ExecutorStrategies.getInstance(session, instanceName));
  }
}
