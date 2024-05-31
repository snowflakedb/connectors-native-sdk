/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import com.snowflake.connectors.application.configuration.finalization.FinalizeConnectorHandler;
import com.snowflake.connectors.application.scheduler.SchedulerCreator;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.connection.GithubConnectionConfigurationCallback;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION} procedure,
 * created using a custom implementation of {@link FinalizeConnectorHandler}.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * GithubConnectionConfigurationCallback} first.
 */
public class FinalizeConnectorConfigurationCustomHandler {

  public Variant finalizeConnectorConfiguration(Session session, Variant customConfig) {
    var schedulerCreator = SchedulerCreator.getInstance(session);
    var handler =
        FinalizeConnectorHandler.builder(session)
            .withCallback(new FinalizeConnectorConfigurationInternal(session, schedulerCreator))
            .withSourceValidator(new GitHubAccessValidator())
            .withInputValidator(v -> ConnectorResponse.success())
            .build();
    return handler.finalizeConnectorConfiguration(customConfig).toVariant();
  }
}
