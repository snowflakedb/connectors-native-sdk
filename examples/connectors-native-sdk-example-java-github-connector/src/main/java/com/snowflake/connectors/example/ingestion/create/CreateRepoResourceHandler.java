/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.create;

import com.snowflake.connectors.application.ingestion.create.CreateResourceHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.CREATE_RESOURCE} procedure, created using a
 * custom implementation of {@link CreateResourceHandler}.
 */
public class CreateRepoResourceHandler {

  public static Variant createResource(
      Session session,
      String name,
      Variant resourceId,
      Variant ingestionConfigurations,
      String id,
      boolean enabled,
      Variant resourceMetadata) {
    var handler =
        CreateResourceHandler.builder(session)
            .withCreateResourceValidator(variantResource -> ConnectorResponse.success())
            .withPreCreateResourceCallback(variantResource -> ConnectorResponse.success())
            .withPostCreateResourceCallback(
                resourceIngestionDefinitionId -> ConnectorResponse.success())
            .build();
    return handler
        .createResource(id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations)
        .toVariant();
  }
}
