/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import com.snowflake.connectors.application.configuration.finalization.FinalizeConnectorHandler;
import com.snowflake.connectors.application.scheduler.SchedulerManager;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.connection.TemplateConnectionConfigurationCallback;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION} procedure,
 * created using a custom implementation of {@link FinalizeConnectorHandler}.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * TemplateConnectionConfigurationCallback} first.
 */
public class TemplateFinalizeConnectorConfigurationCustomHandler {

  public Variant finalizeConnectorConfiguration(Session session, Variant customConfig) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide them here to
    // handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/finalize_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/finalize_configuration
    var schedulerManager = SchedulerManager.getInstance(session);
    var handler =
        FinalizeConnectorHandler.builder(session)
            .withCallback(
                new TemplateFinalizeConnectorConfigurationInternal(session, schedulerManager))
            .withSourceValidator(new TemplateAccessValidator())
            .withInputValidator(v -> ConnectorResponse.success())
            .build();
    return handler.finalizeConnectorConfiguration(customConfig).toVariant();
  }
}
