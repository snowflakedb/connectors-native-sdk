/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import com.snowflake.connectors.application.configuration.finalization.SourceValidator;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link SourceValidator}, used by the {@link
 * TemplateFinalizeConnectorConfigurationCustomHandler}, providing final validation external source
 * system.
 */
public class TemplateAccessValidator implements SourceValidator {

  @Override
  public ConnectorResponse validate(Variant variant) {
    // TODO: IMPLEMENT ME validate source: Implement the custom logic of validating the source
    // system. In some cases this can be the same validation that happened in
    // TemplateConnectionValidator.
    // However, it is suggested to perform more complex validations, like specific access rights to
    // some specific resources here.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/finalize_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/finalize_configuration
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for IMPLEMENT ME validate source");
  }
}
