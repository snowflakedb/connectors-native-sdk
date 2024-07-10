/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.enable;

import com.snowflake.connectors.application.ingestion.enable.PreEnableResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link PreEnableResourceCallback}, used by the {@link
 * TemplateEnableResourceHandler}.
 */
public class TemplatePreEnableResourceCallback implements PreEnableResourceCallback {

  @Override
  public ConnectorResponse execute(String resourceIngestionConfigurationId) {
    // TODO: IMPLEMENT ME pre enable resource callback: If it is required to do some custom
    // operations before the resource is enabled, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/enable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/enable_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME pre enable resource"
            + " callback'");
  }
}
