/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.TEST_CONNECTION} procedure, used by the
 * {@link ConnectionConfigurationHandler} for initial external connection testing.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * TemplateConnectionConfigurationCallback} first.
 */
public class TemplateConnectionValidator {

  public static Variant testConnection(Session session) {
    return testConnection().toVariant();
  }

  private static ConnectorResponse testConnection() {
    // TODO: IMPLEMENT ME test connection: Implement the custom logic of testing the connection to
    // the source system here. This usually requires connection to some webservice or other external
    // system. It is suggested to perform only the basic connectivity validation here.
    // If that's the case then this procedure must be altered in
    // TemplateConnectionConfigurationCallback first.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME test connection'");
  }
}
