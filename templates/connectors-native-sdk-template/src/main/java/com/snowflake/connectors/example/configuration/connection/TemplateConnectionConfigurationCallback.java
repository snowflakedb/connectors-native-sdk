/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationCallback}, used by the {@link
 * TemplateConnectionConfigurationHandler}, providing external access configuration for the
 * connector procedures.
 */
public class TemplateConnectionConfigurationCallback implements ConnectionConfigurationCallback {

  private final Session session;

  public TemplateConnectionConfigurationCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute(Variant config) {
    // TODO: If you need to alter some procedures with external access you can use
    // configureProcedure method or implement a similar method on your own.
    // TODO: IMPLEMENT ME connection callback: Implement the custom logic of changes in application
    // to be done after connection configuration, like altering procedures with external access.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME connection callback'");
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
                "secret variable name",
                configMap.get("key from config with secret name").asString(),
                configMap.get("key from config with external access name").asString()))
        .collect();
  }
}
