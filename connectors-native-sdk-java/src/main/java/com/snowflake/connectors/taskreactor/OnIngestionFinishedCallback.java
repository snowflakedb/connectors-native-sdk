/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import com.snowflake.snowpark_java.types.Variant;

/** Callback called when ingestion is finished. */
public interface OnIngestionFinishedCallback {

  /**
   * Method which is called when Task Reactor finishes an ingestion
   *
   * @param id id of a work item specified when the work item was inserted to the queue
   * @param metadata payload with additional information that can be shared between work items of
   *     the same type
   */
  void onIngestionFinished(String id, Variant metadata);
}
