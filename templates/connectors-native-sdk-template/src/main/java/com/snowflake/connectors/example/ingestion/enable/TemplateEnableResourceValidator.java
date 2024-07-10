/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion.enable;

import com.snowflake.connectors.application.ingestion.enable.EnableResourceValidator;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link EnableResourceValidator}, used by the {@link
 * TemplateEnableResourceHandler}.
 */
public class TemplateEnableResourceValidator implements EnableResourceValidator {

  @Override
  public ConnectorResponse validate(String resourceIngestionConfigurationId) {
    // TODO: IMPLEMENT ME enable resource validate: If there is a validation required of an enabled
    // resource it is a right place to implement the validation logic.
    // See more in docs:
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/flow/ingestion-management/enable_resource
    // https://docs.snowflake.com/en/developer-guide/native-apps/connector-sdk/reference/enable_resource_reference
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME enable resource validate'");
  }
}
