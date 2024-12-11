/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResetConfigurationValidator}. */
class DefaultResetConfigurationValidator implements ResetConfigurationValidator {

  private final Session session;

  DefaultResetConfigurationValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate() {
    return callPublicProcedure(session, "RESET_CONFIGURATION_VALIDATE");
  }
}
