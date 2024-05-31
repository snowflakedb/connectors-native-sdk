/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationHandler;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.SET_CONNECTION_CONFIGURATION} procedure,
 * created using a custom implementation of {@link ConnectionConfigurationHandler}.
 */
public class TemplateConnectionConfigurationHandler {

  public static Variant setConnectionConfiguration(Session session, Variant config) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide them here to
    // handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration
    var handler =
        ConnectionConfigurationHandler.builder(session)
            .withInputValidator(new TemplateConnectionConfigurationInputValidator())
            .withCallback(new TemplateConnectionConfigurationCallback(session))
            .build();
    return handler.setConnectionConfiguration(config).toVariant();
  }
}
