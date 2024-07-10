/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.SET_CONNECTION_CONFIGURATION} procedure,
 * created using a custom implementation of {@link ConnectionConfigurationHandler}.
 */
public class GithubConnectionConfigurationHandler {

  public static Variant setConnectionConfiguration(Session session, Variant config) {
    var handler =
        ConnectionConfigurationHandler.builder(session)
            .withInputValidator(cfg -> ConnectorResponse.success())
            .withCallback(new GithubConnectionConfigurationCallback(session))
            .build();
    return handler.setConnectionConfiguration(config).toVariant();
  }
}
