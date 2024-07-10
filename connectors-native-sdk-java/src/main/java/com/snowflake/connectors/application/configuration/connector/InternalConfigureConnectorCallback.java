/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link ConfigureConnectorCallback}. */
class InternalConfigureConnectorCallback implements ConfigureConnectorCallback {

  private final Session session;

  InternalConfigureConnectorCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant configuration) {
    return callPublicProcedure(session, "CONFIGURE_CONNECTOR_INTERNAL", asVariant(configuration));
  }
}
