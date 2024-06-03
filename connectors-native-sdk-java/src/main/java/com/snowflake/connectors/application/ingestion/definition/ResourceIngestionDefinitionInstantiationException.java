/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

/** Exception thrown when instantiation of an ingestion definition class failed. */
public class ResourceIngestionDefinitionInstantiationException extends RuntimeException {

  /**
   * Creates a new {@link ResourceIngestionDefinitionInstantiationException}.
   *
   * @param message exception message
   * @param cause exception cause
   */
  public ResourceIngestionDefinitionInstantiationException(String message, Throwable cause) {
    super(message, cause);
  }
}
