/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.create;

import com.snowflake.connectors.application.ingestion.create.PostCreateResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link PostCreateResourceCallback}, used by the {@link
 * TemplateCreateResourceHandler}.
 */
public class TemplatePostCreateResourceCallback implements PostCreateResourceCallback {

  @Override
  public ConnectorResponse execute(String resourceIngestionDefinitionId) {
    // TODO: IMPLEMENT ME post create resource callback: If it is required to do some custom
    // operations after the
    // resource is created, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/create_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/create_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME post create resource"
            + " callback'");
  }
}
