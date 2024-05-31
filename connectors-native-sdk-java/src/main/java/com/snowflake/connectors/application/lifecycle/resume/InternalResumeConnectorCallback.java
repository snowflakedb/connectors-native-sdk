/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResumeConnectorCallback}. */
class InternalResumeConnectorCallback implements ResumeConnectorCallback {

  private final Session session;

  InternalResumeConnectorCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute() {
    return callPublicProcedure(session, "RESUME_CONNECTOR_INTERNAL");
  }
}
