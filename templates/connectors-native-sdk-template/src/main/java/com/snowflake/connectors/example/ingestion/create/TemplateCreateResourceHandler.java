/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.create;

import com.snowflake.connectors.application.ingestion.create.CreateResourceHandler;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.CREATE_RESOURCE} procedure, created using a
 * custom implementation of {@link CreateResourceHandler}.
 */
public class TemplateCreateResourceHandler {

  public static Variant createResource(
      Session session,
      String name,
      Variant resourceId,
      Variant ingestionConfigurations,
      String id,
      boolean enabled,
      Variant resourceMetadata) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide
    // them here to handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/create_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/create_resource_reference
    var handler =
        CreateResourceHandler.builder(session)
            .withCreateResourceValidator(new TemplateCreateResourceValidator())
            .withPreCreateResourceCallback(new TemplatePreCreateResourceCallback())
            .withPostCreateResourceCallback(new TemplatePostCreateResourceCallback())
            .build();
    return handler
        .createResource(id, name, enabled, resourceId, resourceMetadata, ingestionConfigurations)
        .toVariant();
  }
}
