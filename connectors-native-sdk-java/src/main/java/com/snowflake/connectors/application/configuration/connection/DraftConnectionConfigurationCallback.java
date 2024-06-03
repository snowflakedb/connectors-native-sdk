/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.sql.SqlTools.variantArgument;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Draft connection implementation of {@link ConnectionConfigurationCallback}. */
class DraftConnectionConfigurationCallback implements ConnectionConfigurationCallback {

  private final Session session;

  DraftConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant configuration) {
    return callPublicProcedure(
        session, "DRAFT_CONNECTION_CONFIGURATION_INTERNAL", variantArgument(configuration));
  }
}
