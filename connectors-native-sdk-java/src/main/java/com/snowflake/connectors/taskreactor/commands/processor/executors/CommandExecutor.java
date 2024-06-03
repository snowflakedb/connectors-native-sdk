/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import com.snowflake.connectors.taskreactor.commands.queue.Command;

/** Task Reactor command executor. */
public interface CommandExecutor {

  /**
   * Executes the logic required by the command passed as an argument.
   *
   * @param command command to be executed
   */
  void execute(Command command);
}
