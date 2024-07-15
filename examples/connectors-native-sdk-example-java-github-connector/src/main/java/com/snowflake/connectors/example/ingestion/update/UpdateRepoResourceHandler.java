/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.update;

import com.snowflake.connectors.application.ingestion.update.UpdateResourceHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.UPDATE_RESOURCE} procedure, created using a
 * custom implementation of {@link UpdateResourceHandler}.
 */
public class UpdateRepoResourceHandler {

  public static Variant updateResource(
      Session session, String resourceIngestionConfigurationId, Variant ingestionConfigurations) {
    var handler =
        UpdateResourceHandler.builder(session)
            .withUpdateResourceValidator(
                (resourceIngestionDefinition, updatedIngestionConfigurations) ->
                    ConnectorResponse.success())
            .withPreUpdateResourceCallback(
                (resourceIngestionDefinition, updatedIngestionConfigurations) ->
                    ConnectorResponse.success())
            .withPostUpdateResourceCallback(
                (resourceIngestionDefinition, updatedIngestionConfigurations) ->
                    ConnectorResponse.success())
            .build();
    return handler
        .updateResource(resourceIngestionConfigurationId, ingestionConfigurations)
        .toVariant();
  }
}
