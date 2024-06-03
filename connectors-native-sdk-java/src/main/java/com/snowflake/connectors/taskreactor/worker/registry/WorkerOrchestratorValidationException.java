/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

/** Exception thrown when an invalid number of workers is specified. */
public class WorkerOrchestratorValidationException extends RuntimeException {

  /**
   * Creates a new {@link WorkerOrchestratorValidationException}.
   *
   * @param message exception message
   */
  public WorkerOrchestratorValidationException(String message) {
    super(message);
  }
}
