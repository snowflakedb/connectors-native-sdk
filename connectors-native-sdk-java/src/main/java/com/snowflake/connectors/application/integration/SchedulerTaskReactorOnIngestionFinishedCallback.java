/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.integration;

import com.snowflake.connectors.application.scheduler.OnIngestionFinishedCallback;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Task reactor compatible implementation of {@link OnIngestionFinishedCallback}. */
public class SchedulerTaskReactorOnIngestionFinishedCallback
    implements com.snowflake.connectors.taskreactor.OnIngestionFinishedCallback {

  private final OnIngestionFinishedCallback schedulerCallback;

  /**
   * Creates a new {@link SchedulerTaskReactorOnIngestionFinishedCallback}.
   *
   * @param schedulerCallback scheduler callback for ingestion finish
   */
  public SchedulerTaskReactorOnIngestionFinishedCallback(
      OnIngestionFinishedCallback schedulerCallback) {
    this.schedulerCallback = schedulerCallback;
  }

  @Override
  public void onIngestionFinished(String ingestionProcessId, Variant metadata) {
    schedulerCallback.onIngestionFinished(ingestionProcessId, metadata);
  }

  /**
   * Returns a new instance of the default callback implementation.
   *
   * <p>Default implementation of the callback uses a default implementation of {@link
   * OnIngestionFinishedCallback}.
   *
   * @param session Snowpark session object
   * @return a new callback instance
   */
  public static SchedulerTaskReactorOnIngestionFinishedCallback getInstance(Session session) {
    var schedulerCallback = OnIngestionFinishedCallback.getInstance(session);
    return new SchedulerTaskReactorOnIngestionFinishedCallback(schedulerCallback);
  }
}
