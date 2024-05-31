/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;
import static com.snowflake.connectors.util.sql.SqlTools.variantArgument;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link FinalizeConnectorInputValidator}. */
class DefaultFinalizeConnectorInputValidator implements FinalizeConnectorInputValidator {

  private final Session session;

  DefaultFinalizeConnectorInputValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(Variant configuration) {
    return callPublicProcedure(
        session, "FINALIZE_CONNECTOR_CONFIGURATION_VALIDATE", variantArgument(configuration));
  }
}
