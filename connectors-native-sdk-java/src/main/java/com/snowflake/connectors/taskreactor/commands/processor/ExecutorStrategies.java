/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.processor.executors.CommandExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.snowpark_java.Session;
import java.util.Optional;

/**
 * The strategy that defines which {@link CommandExecutor} implementation is responsible for
 * execution of command of a particular type.
 */
public interface ExecutorStrategies {

  /**
   * Provides the proper implementation of {@link CommandExecutor} for the given value of {@link
   * CommandType}
   *
   * @param commandType command type
   * @return command executor for given command type
   */
  Optional<CommandExecutor> getStrategy(CommandType commandType);

  /**
   * Returns a new instance of the default {@link ExecutorStrategies} implementation used by the
   * Task Reactor.
   *
   * @param session Snowpark session object
   * @param instanceName Task Reactor instance name
   * @return a new default commands executors strategy instance
   */
  static ExecutorStrategies getInstance(Session session, Identifier instanceName) {
    return new DefaultExecutorsStrategies(session, instanceName);
  }
}
