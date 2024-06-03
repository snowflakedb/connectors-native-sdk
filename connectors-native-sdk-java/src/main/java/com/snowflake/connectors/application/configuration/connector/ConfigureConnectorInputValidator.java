/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * A validator for the input of {@link ConfigureConnectorHandler}, may be used to provide custom
 * connector configuration validation.
 *
 * <p>Default implementation of this validator calls the {@code PUBLIC.CONFIGURE_CONNECTOR_VALIDATE}
 * procedure.
 */
@FunctionalInterface
public interface ConfigureConnectorInputValidator {

  /**
   * Validates the provided connector configuration.
   *
   * @param configuration connector configuration provided to the handler
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Variant configuration);
}
