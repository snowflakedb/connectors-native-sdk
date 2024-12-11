/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.pause;

import com.snowflake.connectors.application.lifecycle.pause.PauseConnectorCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link PauseConnectorCallback}, used by the {@link
 * PauseConnectorCustomHandler}, providing suspension of the scheduler system.
 */
public class InternalPauseConnectorCallback implements PauseConnectorCallback {

  @Override
  public ConnectorResponse execute() {
    return ConnectorResponse.success();
  }
}
