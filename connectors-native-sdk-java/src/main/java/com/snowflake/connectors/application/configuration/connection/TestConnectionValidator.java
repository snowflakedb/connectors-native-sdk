/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.util.sql.SqlTools.callPublicProcedure;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ConnectionValidator}. */
class TestConnectionValidator implements ConnectionValidator {

  private final Session session;

  TestConnectionValidator(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse validate() {
    return callPublicProcedure(session, "TEST_CONNECTION");
  }
}
