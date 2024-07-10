/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link OnIngestionScheduledCallback}. */
class DefaultOnIngestionScheduledCallback implements OnIngestionScheduledCallback {

  private final Session session;

  DefaultOnIngestionScheduledCallback(Session session) {
    this.session = session;
  }

  @Override
  public void onIngestionScheduled(String processId) {
    callPublicProcedure(session, "ON_INGESTION_SCHEDULED", asVarchar(processId));
  }
}
