/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Callback called during the {@link ConfigureConnectorHandler} execution, may be used to provide
 * custom connector configuration logic.
 *
 * <p>Default implementation of this callback calls the {@code PUBLIC.CONFIGURE_CONNECTOR_INTERNAL}
 * procedure.
 */
@FunctionalInterface
public interface ConfigureConnectorCallback {

  /**
   * Executes logic using the provided connector configuration.
   *
   * @param configuration connector configuration provided to the handler
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(Variant configuration);
}
