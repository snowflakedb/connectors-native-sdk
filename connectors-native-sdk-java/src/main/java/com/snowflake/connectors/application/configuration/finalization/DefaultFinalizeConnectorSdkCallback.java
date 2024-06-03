/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

class DefaultFinalizeConnectorSdkCallback implements FinalizeConnectorSdkCallback {

  private final Session session;

  DefaultFinalizeConnectorSdkCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute() {
    return ConnectorResponse.success();
  }
}
