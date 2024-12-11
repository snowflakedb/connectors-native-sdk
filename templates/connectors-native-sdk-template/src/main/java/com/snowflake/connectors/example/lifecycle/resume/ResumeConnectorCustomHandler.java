/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.resume;

import com.snowflake.connectors.application.lifecycle.resume.ResumeConnectorHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/** Backend implementation for the custom {@code PUBLIC.RESUME_CONNECTOR} procedure. */
public class ResumeConnectorCustomHandler {

  public Variant resumeConnector(Session session) {
    var internalCallback = new InternalResumeConnectorCallback();
    var handler =
        ResumeConnectorHandler.builder(session)
            .withStateValidator(ConnectorResponse::success)
            .withCallback(internalCallback)
            .build();
    return handler.resumeConnector().toVariant();
  }
}
