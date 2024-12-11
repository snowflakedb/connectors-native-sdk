/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResetConfigurationCallback}. */
class InternalResetConfigurationCallback implements ResetConfigurationCallback {

  private final Session session;

  InternalResetConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute() {
    return callPublicProcedure(session, "RESET_CONFIGURATION_INTERNAL");
  }
}
