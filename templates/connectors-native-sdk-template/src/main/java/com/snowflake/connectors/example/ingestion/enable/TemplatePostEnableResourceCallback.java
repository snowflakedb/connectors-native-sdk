/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.enable;

import com.snowflake.connectors.application.ingestion.enable.PostEnableResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link PostEnableResourceCallback}, used by the {@link
 * TemplateEnableResourceHandler}.
 */
public class TemplatePostEnableResourceCallback implements PostEnableResourceCallback {

  @Override
  public ConnectorResponse execute(String resourceIngestionConfigurationId) {
    // TODO: IMPLEMENT ME post enable resource callback: If it is required to do some custom
    // operations after the resource is enabled, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/enable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/enable_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME post enable resource"
            + " callback'");
  }
}
