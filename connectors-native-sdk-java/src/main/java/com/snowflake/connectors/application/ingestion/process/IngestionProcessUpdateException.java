/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

/** Exception thrown when more or less than one ingestion process row is updated. */
public class IngestionProcessUpdateException extends RuntimeException {

  /**
   * Creates a new {@link IngestionProcessUpdateException}.
   *
   * @param rowsUpdated number of updated rows
   */
  public IngestionProcessUpdateException(Long rowsUpdated) {
    super(
        String.format(
            "Precisely 1 row should be updated in ingestion_process. Number of updated rows: %s",
            rowsUpdated));
  }
}
