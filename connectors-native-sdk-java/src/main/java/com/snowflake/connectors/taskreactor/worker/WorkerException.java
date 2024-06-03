/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

/** Exception thrown when a worker fails. */
public class WorkerException extends Exception {

  /**
   * Creates a new {@link WorkerException}.
   *
   * @param message exception message
   * @param cause exception cause
   */
  WorkerException(String message, Throwable cause) {
    super(message, cause);
  }
}
