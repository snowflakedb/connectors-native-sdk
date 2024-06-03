/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability.exception;

import com.snowflake.connectors.application.observability.IngestionRun;

/** Exception thrown when an invalid status is provided when ending an ingestion run. */
public class IngestionRunsBadStatusException extends RuntimeException {

  /**
   * Creates a new {@link IngestionRunsBadStatusException}.
   *
   * @param status provided ingestion run status
   */
  public IngestionRunsBadStatusException(IngestionRun.IngestionStatus status) {
    super(
        String.format(
            "Status when ending ingestion run should be one of: %s, %s, %s. Provided status was:"
                + " %s",
            IngestionRun.IngestionStatus.COMPLETED,
            IngestionRun.IngestionStatus.CANCELED,
            IngestionRun.IngestionStatus.FAILED,
            status));
  }
}
