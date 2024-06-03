/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.snowpark_java.Session;

/**
 * Callback called when the next scheduler iteration is run.
 *
 * <p>Default implementation of this callback calls the {@code PUBLIC.ON_INGESTION_SCHEDULED}
 * procedure.
 */
public interface OnIngestionScheduledCallback {

  /**
   * Action executed when the next scheduler iteration is run.
   *
   * @param processId ingestion process id
   */
  void onIngestionScheduled(String processId);

  /**
   * Returns a new instance of the default callback implementation.
   *
   * @param session Snowpark session object
   * @return a new callback instance
   */
  static OnIngestionScheduledCallback getInstance(Session session) {
    return new DefaultOnIngestionScheduledCallback(session);
  }
}
