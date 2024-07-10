/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.disable;

import com.snowflake.connectors.application.ingestion.disable.DisableResourceHandler;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.DISABLE_RESOURCE} procedure, created using a
 * custom implementation of {@link DisableResourceHandler}.
 */
public class TemplateDisableResourceHandler {

  public static Variant disableResource(Session session, String resourceIngestionConfigurationId) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide them here to
    // handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/disable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/disable_resource_reference
    var handler =
        DisableResourceHandler.builder(session)
            .withPreDisableResourceCallback(new TemplatePreDisableResourceCallback())
            .withPostDisableResourceCallback(new TemplatePostDisableResourceCallback())
            .build();
    return handler.disableResource(resourceIngestionConfigurationId).toVariant();
  }
}
