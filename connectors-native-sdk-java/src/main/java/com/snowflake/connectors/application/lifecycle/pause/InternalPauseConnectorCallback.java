/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link PauseConnectorCallback} */
class InternalPauseConnectorCallback implements PauseConnectorCallback {

  private final Session session;

  InternalPauseConnectorCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute() {
    return callPublicProcedure(session, "PAUSE_CONNECTOR_INTERNAL");
  }
}
