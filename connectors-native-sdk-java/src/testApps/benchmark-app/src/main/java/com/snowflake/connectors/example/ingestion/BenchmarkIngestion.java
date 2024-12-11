/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.COMPLETED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.FAILED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.IN_PROGRESS;

import com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus;
import com.snowflake.connectors.taskreactor.worker.ingestion.Ingestion;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import java.time.Duration;
import java.util.Random;

/** Custom implementation of {@link Ingestion}, used for ingestion of random data */
public class BenchmarkIngestion implements Ingestion<IngestionStatus> {

  Random random = new Random();

  public BenchmarkIngestion() {}

  @Override
  public IngestionStatus initialState(WorkItem workItem) {
    return IN_PROGRESS;
  }

  @Override
  public void preIngestion(WorkItem workItem) {}

  @Override
  public IngestionStatus performIngestion(WorkItem workItem, IngestionStatus ingestionStatus) {
    try {
      int randomSeconds = random.nextInt(5) + 5;
      Thread.sleep(Duration.ofSeconds(randomSeconds).toMillis());
      return COMPLETED;
    } catch (Exception exception) {
      return FAILED;
    }
  }

  @Override
  public boolean isIngestionCompleted(WorkItem workItem, IngestionStatus status) {
    return status != IN_PROGRESS;
  }

  @Override
  public void postIngestion(WorkItem workItem, IngestionStatus ingestionStatus) {}

  @Override
  public void ingestionCancelled(WorkItem workItem, IngestionStatus lastState) {}
}
