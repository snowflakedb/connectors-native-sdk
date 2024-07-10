/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link FinalizeConnectorCallback}. */
class InternalFinalizeConnectorCallback implements FinalizeConnectorCallback {

  private final Session session;

  InternalFinalizeConnectorCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant configuration) {
    return callPublicProcedure(
        session, "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL", asVariant(configuration));
  }
}
