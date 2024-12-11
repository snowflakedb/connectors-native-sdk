/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.sql.SqlTools;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Default implementation of {@link ConnectionConfigurationInputValidator} for {@code
 * PUBLIC.SET_CONNECTION_CONFIGURATION_VALIDATE} procedure.
 */
class DefaultSetConnectionConfigurationInputValidator
    implements ConnectionConfigurationInputValidator {

  private final Session session;

  DefaultSetConnectionConfigurationInputValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate(Variant connectionConfiguration) {
    return callPublicProcedure(
        session,
        "SET_CONNECTION_CONFIGURATION_VALIDATE",
        SqlTools.asVariant(connectionConfiguration));
  }
}
