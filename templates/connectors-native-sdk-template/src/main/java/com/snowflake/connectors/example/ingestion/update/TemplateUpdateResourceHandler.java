/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.update;

import com.snowflake.connectors.application.ingestion.update.UpdateResourceHandler;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.UPDATE_RESOURCE} procedure, created using a
 * custom implementation of {@link UpdateResourceHandler}.
 */
public class TemplateUpdateResourceHandler {

  public static Variant updateResource(
      Session session, String resourceIngestionConfigurationId, Variant ingestionConfigurations) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide them here to
    // handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/updated_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/update_resource_reference
    var handler =
        UpdateResourceHandler.builder(session)
            .withUpdateResourceValidator(new TemplateUpdateResourceValidator())
            .withPreUpdateResourceCallback(new TemplatePreUpdateResourceCallback())
            .withPostUpdateResourceCallback(new TemplatePostUpdateResourceCallback())
            .build();
    return handler
        .updateResource(resourceIngestionConfigurationId, ingestionConfigurations)
        .toVariant();
  }
}
