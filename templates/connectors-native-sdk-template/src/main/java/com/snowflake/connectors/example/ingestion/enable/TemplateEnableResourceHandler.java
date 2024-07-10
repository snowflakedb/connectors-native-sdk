/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.enable;

import com.snowflake.connectors.application.ingestion.enable.EnableResourceHandler;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.ENABLE_RESOURCE} procedure, created using a
 * custom implementation of {@link EnableResourceHandler}.
 */
public class TemplateEnableResourceHandler {

  public static Variant enableResource(Session session, String resourceIngestionConfigurationId) {
    // TODO: HINT: If you want to implement the interfaces yourself you need to provide them here to
    // handler or specify your own handler.
    // This method is referenced with full classpath from the `setup.sql` script.
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/enable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/enable_resource_reference
    var handler =
        EnableResourceHandler.builder(session)
            .withEnableResourceValidator(new TemplateEnableResourceValidator())
            .withPreEnableResourceCallback(new TemplatePreEnableResourceCallback())
            .withPostEnableResourceCallback(new TemplatePostEnableResourceCallback())
            .build();
    return handler.enableResource(resourceIngestionConfigurationId).toVariant();
  }
}
