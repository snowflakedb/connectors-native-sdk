/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationInputValidator;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link ConnectionConfigurationInputValidator}, used by the {@link
 * TemplateConnectionConfigurationHandler}.
 */
public class TemplateConnectionConfigurationInputValidator
    implements ConnectionConfigurationInputValidator {

  @Override
  public ConnectorResponse validate(Variant config) {
    // TODO: IMPLEMENT ME connection configuration validate: If the connection configuration input
    // requires some additional validation this is the place to implement this logic.
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/connection_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/connection_configuration
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for 'IMPLEMENT ME connection configuration"
            + " validate'");
  }
}
