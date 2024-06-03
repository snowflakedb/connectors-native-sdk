/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

/** Test class of the SDK's ConnectorStatus */
public enum ConnectorStatus {
  CONFIGURING,
  STARTING,
  STARTED,
  PAUSING,
  PAUSED,
  ERROR;

  public enum ConnectorConfigurationStatus {
    INSTALLED,
    PREREQUISITES_DONE,
    CONFIGURED,
    CONNECTED,
    FINALIZED;
  }
}
