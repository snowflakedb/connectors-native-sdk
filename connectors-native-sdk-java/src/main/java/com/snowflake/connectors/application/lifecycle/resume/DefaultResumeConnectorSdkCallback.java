/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link ResumeConnectorSdkCallback}. */
class DefaultResumeConnectorSdkCallback implements ResumeConnectorSdkCallback {

  private final Session session;

  DefaultResumeConnectorSdkCallback(Session session) {
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
  static DefaultResumeConnectorSdkCallback getInstance(Session session) {
    return new DefaultResumeConnectorSdkCallback(session);
  }
}
