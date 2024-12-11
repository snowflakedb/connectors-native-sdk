/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Callback called during the {@link ResetConfigurationHandler} execution, may be used to provide
 * custom reset configuration logic.
 *
 * <p>Default implementation of this callback calls the {@code PUBLIC.RESET_CONFIGURATION_INTERNAL}
 * procedure.
 */
@FunctionalInterface
public interface ResetConfigurationCallback {

  /**
   * Executes custom reset configuration logic.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute();
}
