/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.enable;

import com.snowflake.connectors.application.ingestion.enable.EnableResourceHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.ENABLE_RESOURCE} procedure, created using a
 * custom implementation of {@link EnableResourceHandler}.
 */
public class EnableRepoResourceHandler {

  public static Variant enableResource(Session session, String resourceIngestionConfigurationId) {
    var handler =
        EnableResourceHandler.builder(session)
            .withEnableResourceValidator(resourceIngestionDefinition -> ConnectorResponse.success())
            .withPreEnableResourceCallback(
                resourceIngestionDefinition -> ConnectorResponse.success())
            .withPostEnableResourceCallback(
                resourceIngestionDefinition -> ConnectorResponse.success())
            .build();
    return handler.enableResource(resourceIngestionConfigurationId).toVariant();
  }
}
