/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link PauseConnectorSdkCallback}. */
class DefaultPauseConnectorSdkCallback implements PauseConnectorSdkCallback {

  private final Session session;

  DefaultPauseConnectorSdkCallback(Session session) {
    this.session = session;
  }

  @Override
  public ConnectorResponse execute() {
    return ConnectorResponse.success();
  }

  /**
   * Creates new instance of the default callback, which does not perform any operation.
   *
   * @param session Snowpark session object
   * @return new callback instance
   */
  static DefaultPauseConnectorSdkCallback getInstance(Session session) {
    return new DefaultPauseConnectorSdkCallback(session);
  }
}
