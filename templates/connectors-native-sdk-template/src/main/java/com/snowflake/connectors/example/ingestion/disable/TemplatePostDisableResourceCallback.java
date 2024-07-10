/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.disable;

import com.snowflake.connectors.application.ingestion.disable.PostDisableResourceCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link PostDisableResourceCallback}, used by the {@link
 * TemplateDisableResourceHandler}.
 */
public class TemplatePostDisableResourceCallback implements PostDisableResourceCallback {

  @Override
  public ConnectorResponse execute(String resourceIngestionConfigurationId) {
    // TODO: IMPLEMENT ME post disable resource callback: If it is required to do some custom
    // operations after the resource is disabled, it is a proper place to implement the logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/disable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/disable_resource_reference
    return ConnectorResponse.success();
  }
}
