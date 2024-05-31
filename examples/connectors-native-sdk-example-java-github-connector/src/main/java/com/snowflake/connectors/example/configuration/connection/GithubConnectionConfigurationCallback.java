/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.ConnectorObjects.FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.PUBLIC_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.TEST_CONNECTION_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.WORKER_PROCEDURE;
import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.INTEGRATION_PARAM;
import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.SECRET_PARAM;
import static com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration.TOKEN_NAME;
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

  public GithubConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant config) {
    configureProcedure(format("%s.%s()", PUBLIC_SCHEMA, TEST_CONNECTION_PROCEDURE), config);
    configureProcedure(
        format("%s.%s(VARIANT)", PUBLIC_SCHEMA, FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE),
        config);
    configureProcedure(format("%s.%s(NUMBER, STRING)", PUBLIC_SCHEMA, WORKER_PROCEDURE), config);

    return ConnectorResponse.success();
  }

  private void configureProcedure(String procedureName, Variant config) {
    var configMap = config.asMap();

    session
        .sql(
            format(
                "ALTER PROCEDURE %s SET "
                    + "SECRETS=('%s' = %s) "
                    + "EXTERNAL_ACCESS_INTEGRATIONS=(%s)",
                procedureName,
                TOKEN_NAME,
                configMap.get(SECRET_PARAM).asString(),
                configMap.get(INTEGRATION_PARAM).asString()))
        .collect();
  }
}
