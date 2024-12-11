/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.util.sql.SqlTools.arrayConstruct;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Default implementation of {@link OnIngestionScheduledCallback}. */
class DefaultOnIngestionScheduledCallback implements OnIngestionScheduledCallback {

  private final Session session;

  DefaultOnIngestionScheduledCallback(Session session) {
    this.session = session;
  }

  @Override
  public void onIngestionScheduled(List<String> processIds) {
    callPublicProcedure(session, "ON_INGESTION_SCHEDULED", arrayConstruct(processIds));
  }
}
