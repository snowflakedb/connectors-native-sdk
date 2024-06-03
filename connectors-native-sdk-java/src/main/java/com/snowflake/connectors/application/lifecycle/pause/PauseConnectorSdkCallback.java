/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Callback called during the {@link PauseConnectorHandler} execution, may be used to provide custom
 * pausing logic required by the sdk.
 */
public interface PauseConnectorSdkCallback {

  /**
   * Executes the callback logic.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message.
   */
  ConnectorResponse execute();
}
