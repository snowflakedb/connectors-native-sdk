/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability.exception;

/** Exception thrown when insufficient ingestion information when starting an ingestion run. */
public class IngestionRunsReferenceException extends RuntimeException {

  /** Creates a new {@link IngestionRunsReferenceException}. */
  public IngestionRunsReferenceException() {
    super(
        "Process id must be provided or both resource ingestion definition id and ingestion"
            + " configuration id when inserting an ingestion run");
  }
}
