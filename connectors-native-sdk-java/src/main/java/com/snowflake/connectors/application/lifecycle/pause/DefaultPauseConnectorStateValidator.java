/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link PauseConnectorStateValidator}. */
class DefaultPauseConnectorStateValidator implements PauseConnectorStateValidator {

  private final Session session;

  DefaultPauseConnectorStateValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate() {
    return callPublicProcedure(session, "PAUSE_CONNECTOR_VALIDATE");
  }
}
