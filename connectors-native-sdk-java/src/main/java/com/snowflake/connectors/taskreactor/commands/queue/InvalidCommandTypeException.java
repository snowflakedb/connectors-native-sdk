/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.queue;

import static java.lang.String.format;

import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;

/**
 * Exception thrown when the command type is not included in the predefined set represented by the
 * {@link CommandType}.
 */
public class InvalidCommandTypeException extends RuntimeException {

  /** Template of an exception message. */
  private static final String ERROR_MESSAGE =
      "Command type [%s] is not recognized for Command with id [%s].";

  /**
   * Creates a {@link InvalidCommandTypeException} object with all fields initialized.
   *
   * @param commandType type of the command
   * @param id id of the command
   */
  public InvalidCommandTypeException(String commandType, String id) {
    super(format(ERROR_MESSAGE, commandType, id));
  }
}
