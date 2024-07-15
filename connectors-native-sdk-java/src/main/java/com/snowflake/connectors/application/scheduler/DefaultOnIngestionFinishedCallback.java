/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;

import com.snowflake.connectors.application.ingestion.process.CrudIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link OnIngestionFinishedCallback}. */
class DefaultOnIngestionFinishedCallback implements OnIngestionFinishedCallback {

  private final CrudIngestionProcessRepository ingestionProcessRepository;

  DefaultOnIngestionFinishedCallback(CrudIngestionProcessRepository ingestionProcessRepository) {
    this.ingestionProcessRepository = ingestionProcessRepository;
  }

  @Override
  public void onIngestionFinished(String processId, Variant metadata) {
    IngestionProcess ingestionProcess =
        ingestionProcessRepository
            .fetch(processId)
            .orElseThrow(() -> new RuntimeException("Process does not exist"));

    IngestionProcess updatedProcess = ingestionProcess.withMetadata(metadata);
    if (IN_PROGRESS.equals(ingestionProcess.getStatus())) {
      updatedProcess = updatedProcess.withStatus(SCHEDULED);
    }

    ingestionProcessRepository.save(updatedProcess);
  }
}
