/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Callback called during the {@link FinalizeConnectorHandler} execution, may be used to provide
 * custom finalization logic (e.g. configuration persistence).
 *
 * <p>Default implementation of this callback calls the {@code
 * PUBLIC.FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL} procedure.
 */
@FunctionalInterface
public interface FinalizeConnectorCallback {

  /**
   * Executes logic using the provided configuration.
   *
   * @param configuration custom configuration provided to the handler
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(Variant configuration);
}
