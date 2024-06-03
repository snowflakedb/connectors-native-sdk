/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * A validator for the connection configured during the {@link ConnectionConfigurationHandler} or
 * {@link UpdateConnectionConfigurationHandler} execution. Should be used to check if any connection
 * to the external service can be established.
 *
 * <p>Default implementation of this validator calls the {@code PUBLIC.TEST_CONNECTION} procedure.
 *
 * <p>Additional SQL grants (e.g. to the external access integration) may be needed before
 * connection validation.
 */
@FunctionalInterface
public interface ConnectionValidator {

  /**
   * Validates the connection configured during the handler execution.
   *
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate();
}
