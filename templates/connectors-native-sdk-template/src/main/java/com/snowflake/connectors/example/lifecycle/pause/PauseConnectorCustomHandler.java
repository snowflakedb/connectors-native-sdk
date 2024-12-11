/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.pause;

import com.snowflake.connectors.application.lifecycle.pause.PauseConnectorHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Backend implementation for the custom {@code PUBLIC.PAUSE_CONNECTOR} procedure. */
public class PauseConnectorCustomHandler {

  public Variant pauseConnector(Session session) {
    var internalCallback = new InternalPauseConnectorCallback();
    var handler =
        PauseConnectorHandler.builder(session)
            .withStateValidator(ConnectorResponse::success)
            .withCallback(internalCallback)
            .build();
    return handler.pauseConnector().toVariant();
  }
}
