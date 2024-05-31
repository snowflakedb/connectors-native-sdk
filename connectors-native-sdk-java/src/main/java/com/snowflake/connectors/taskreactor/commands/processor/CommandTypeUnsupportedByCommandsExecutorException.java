/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import static java.lang.String.format;

import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;

/**
 * Exception thrown when the particular value of the {@link CommandType} is not supported by the
 * implementation of the {@link ExecutorStrategies}.
 */
public class CommandTypeUnsupportedByCommandsExecutorException extends RuntimeException {

  /** Template of an exception message. */
  private static final String ERROR_MESSAGE =
      "Command of type [%s] is not supported by the Commands Processor.";

  /**
   * Creates a {@link CommandTypeUnsupportedByCommandsExecutorException} object with all fields
   * initialized
   *
   * @param commandType type of the unsupported command
   */
  public CommandTypeUnsupportedByCommandsExecutorException(CommandType commandType) {
    super(format(ERROR_MESSAGE, commandType.name()));
  }
}
