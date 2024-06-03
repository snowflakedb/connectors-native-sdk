/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Callback called during the {@link FinalizeConnectorHandler} execution, may be used to provide
 * custom finalization logic required by the sdk.
 */
public interface FinalizeConnectorSdkCallback {

  /**
   * Executes logic custom sdk logic.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message.
   */
  ConnectorResponse execute();
}
