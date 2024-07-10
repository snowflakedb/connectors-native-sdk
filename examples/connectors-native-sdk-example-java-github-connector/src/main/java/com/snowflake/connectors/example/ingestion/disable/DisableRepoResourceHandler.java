/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.disable;

import com.snowflake.connectors.application.ingestion.disable.DisableResourceHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.DISABLE_RESOURCE} procedure, created using a
 * custom implementation of {@link DisableResourceHandler}.
 */
public class DisableRepoResourceHandler {

  public static Variant disableResource(Session session, String resourceIngestionConfigurationId) {
    var handler =
        DisableResourceHandler.builder(session)
            .withPreDisableResourceCallback(
                resourceIngestionDefinition -> ConnectorResponse.success())
            .withPostDisableResourceCallback(
                resourceIngestionDefinition -> ConnectorResponse.success())
            .build();
    return handler.disableResource(resourceIngestionConfigurationId).toVariant();
  }
}
