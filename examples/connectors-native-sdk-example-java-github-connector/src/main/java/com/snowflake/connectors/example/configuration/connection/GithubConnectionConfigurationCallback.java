/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.ConnectorObjects.FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.PUBLIC_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.TEST_CONNECTION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.WORKER_PROCEDURE;
import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.TOKEN_NAME;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationCallback;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationCallback}, used by the {@link
 * GithubConnectionConfigurationHandler}, providing external access configuration for the connector
 * procedures.
 */
public class GithubConnectionConfigurationCallback implements ConnectionConfigurationCallback {

  private static final Reference GITHUB_EAI_REFERENCE = Reference.from("GITHUB_EAI_REFERENCE");
  private static final Reference GITHUB_SECRET_REFERENCE =
      Reference.from("GITHUB_SECRET_REFERENCE");

  private final Session session;

  public GithubConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant config) {
    configureProcedure(format("%s.%s()", PUBLIC_SCHEMA, TEST_CONNECTION_PROCEDURE));
    configureProcedure(
        format("%s.%s(VARIANT)", PUBLIC_SCHEMA, FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE));
    configureProcedure(format("%s.%s(NUMBER, STRING)", PUBLIC_SCHEMA, WORKER_PROCEDURE));

    return ConnectorResponse.success();
  }

  private void configureProcedure(String procedureName) {
    session
        .sql(
            format(
                "ALTER PROCEDURE %s SET SECRETS=(%s = %s) EXTERNAL_ACCESS_INTEGRATIONS=(%s)",
                procedureName,
                asVarchar(TOKEN_NAME),
                GITHUB_SECRET_REFERENCE.getValue(),
                GITHUB_EAI_REFERENCE.getValue()))
        .collect();
  }
}
