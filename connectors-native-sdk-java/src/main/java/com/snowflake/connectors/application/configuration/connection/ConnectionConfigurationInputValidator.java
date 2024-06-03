/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * A validator for the input of {@link ConnectionConfigurationHandler} and {@link
 * UpdateConnectionConfigurationHandler}, may be used to provide custom connection configuration
 * validation.
 *
 * <p>Default implementation for setting configuration of this validator calls the {@code
 * PUBLIC.SET_CONNECTION_CONFIGURATION_VALIDATE} procedure.
 *
 * <p>Default implementation for configuration update of this validator calls the {@code
 * PUBLIC.UPDATE_CONNECTION_CONFIGURATION_VALIDATE} procedure.
 */
@FunctionalInterface
public interface ConnectionConfigurationInputValidator {

  /**
   * Validates the provided connection configuration.
   *
   * @param configuration connection configuration provided to the handler
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Variant configuration);
}
