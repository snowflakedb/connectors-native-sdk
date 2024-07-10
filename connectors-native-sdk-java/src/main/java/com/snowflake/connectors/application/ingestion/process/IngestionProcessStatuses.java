/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

/** Class which contains available statuses of {@link IngestionProcess} which are used by the SDK */
public class IngestionProcessStatuses {

  /**
   * Status which means that the next iteration of ingestion for a given process is scheduled and
   * the process will be picked by the scheduler soon
   */
  public static final String SCHEDULED = "SCHEDULED";

  /** Status which means that the ingestion for a given process is being processed at the moment */
  public static final String IN_PROGRESS = "IN_PROGRESS";

  /**
   * Status which means that the process is finished and no more iterations of ingestion will be
   * scheduled
   */
  public static final String FINISHED = "FINISHED";
}
