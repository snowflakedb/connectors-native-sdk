/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Default implementation of {@link ConfigureConnectorInputValidator}. */
class DefaultConfigureConnectorInputValidator implements ConfigureConnectorInputValidator {

  private final Session session;

  DefaultConfigureConnectorInputValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(Variant configuration) {
    return callPublicProcedure(session, "CONFIGURE_CONNECTOR_VALIDATE", asVariant(configuration));
  }
}
