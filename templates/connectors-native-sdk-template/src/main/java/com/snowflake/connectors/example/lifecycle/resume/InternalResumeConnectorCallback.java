/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.resume;

import com.snowflake.connectors.application.lifecycle.resume.ResumeConnectorCallback;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Custom implementation of {@link ResumeConnectorCallback}, used by the {@link
 * ResumeConnectorCustomHandler}, providing resumption of the scheduler system.
 */
public class InternalResumeConnectorCallback implements ResumeConnectorCallback {

  @Override
  public ConnectorResponse execute() {
    return ConnectorResponse.success();
  }
}
