/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResumeConnectorStateValidator}. */
class DefaultResumeConnectorStateValidator implements ResumeConnectorStateValidator {

  private final Session session;

  DefaultResumeConnectorStateValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate() {
    return callPublicProcedure(session, "RESUME_CONNECTOR_VALIDATE");
  }
}
