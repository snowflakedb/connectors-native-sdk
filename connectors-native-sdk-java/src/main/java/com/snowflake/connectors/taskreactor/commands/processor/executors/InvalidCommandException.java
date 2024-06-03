/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor.executors;

import static java.lang.String.format;

import com.snowflake.connectors.taskreactor.commands.queue.Command;

/**
 * Exception thrown when the particular value of the {@link Command} cannot be found during
 * execution process.
 */
public class InvalidCommandException extends RuntimeException {

  private static final String ERROR_MESSAGE = "Command with payload '%s' is invalid.";

  /**
   * Creates a new {@link InvalidCommandException}.
   *
   * @param command invalid command
   */
  public InvalidCommandException(Command command) {
    super(format(ERROR_MESSAGE, command.getPayload().toString()));
  }
}
