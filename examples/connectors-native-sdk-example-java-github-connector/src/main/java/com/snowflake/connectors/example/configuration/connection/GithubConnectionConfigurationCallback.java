/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.ConnectorObjects.FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.PUBLIC_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.SETUP_EXTERNAL_INTEGRATION_WITH_REFS_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.TEST_CONNECTION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.WORKER_PROCEDURE;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callProcedure;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationCallback}, used by the {@link
 * GithubConnectionConfigurationHandler}, providing external access configuration for the connector
 * procedures.
 */
public class GithubConnectionConfigurationCallback implements ConnectionConfigurationCallback {

  private final Session session;
  private static final String[] EXTERNAL_SOURCE_PROCEDURE_SIGNATURES = {
    asVarchar(format("%s.%s()", PUBLIC_SCHEMA, TEST_CONNECTION_PROCEDURE)),
    asVarchar(format("%s.%s(VARIANT)", PUBLIC_SCHEMA, FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE)),
    asVarchar(format("%s.%s(NUMBER, STRING)", PUBLIC_SCHEMA, WORKER_PROCEDURE))
  };

  public GithubConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant config) {
    var response = configureProcedures();
    if (response.isNotOk()) {
      return response;
    }
    return ConnectorResponse.success();
  }

  private ConnectorResponse configureProcedures() {
    return callProcedure(
        session,
        PUBLIC_SCHEMA,
        SETUP_EXTERNAL_INTEGRATION_WITH_REFS_PROCEDURE,
        EXTERNAL_SOURCE_PROCEDURE_SIGNATURES);
  }
}
