/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability.exception;

/** Exception thrown when more or less than one ingestion run row is updated. */
public class IngestionRunsUpdateException extends RuntimeException {

  /**
   * Creates a new {@link IngestionRunsUpdateException}.
   *
   * @param rowsUpdated number of updated rows
   */
  public IngestionRunsUpdateException(Long rowsUpdated) {
    super(
        String.format(
            "Precisely 1 row should be updated in ingestion_runs. Number of updated rows: %s",
            rowsUpdated));
  }
}
