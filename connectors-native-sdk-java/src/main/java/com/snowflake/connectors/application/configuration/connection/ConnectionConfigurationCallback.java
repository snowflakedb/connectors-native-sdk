/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Callback called during the {@link ConnectionConfigurationHandler} and {@link
 * UpdateConnectionConfigurationHandler} execution, may be used to provide custom connection
 * configuration logic.
 *
 * <p>Default implementation of this callback calls the {@code
 * PUBLIC.SET_CONNECTION_CONFIGURATION_INTERNAL} procedure.
 *
 * <p>Draft implementation of this callback calls the {@code
 * PUBLIC.DRAFT_CONNECTION_CONFIGURATION_INTERNAL} procedure.
 */
@FunctionalInterface
public interface ConnectionConfigurationCallback {

  /**
   * Executes logic using the provided connection configuration.
   *
   * @param configuration connection configuration provided to the handler
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(Variant configuration);
}
