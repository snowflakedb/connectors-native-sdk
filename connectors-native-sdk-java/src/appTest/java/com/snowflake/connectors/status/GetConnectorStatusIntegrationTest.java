/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.status;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSING;
import static com.snowflake.connectors.util.ConnectorStatus.STARTING;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import org.junit.jupiter.api.Test;

class GetConnectorStatusIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldReturnConfiguringAndInstalledStatuses() {
    // given
    setConnectorStatus(CONFIGURING, INSTALLED);

    // when
    var procedureResponse = callProcedure("GET_CONNECTOR_STATUS()");

    // then
    assertThatResponseMap(procedureResponse)
        .hasOKResponseCode()
        .hasField("status", "CONFIGURING")
        .hasField("configurationStatus", "INSTALLED");
  }

  @Test
  void shouldReturnStartedStateFromGetConnectorStatusInsteadOfPausing() {
    // given
    setConnectorStatus(PAUSING, FINALIZED);

    // when
    var procedureResponse = callProcedure("GET_CONNECTOR_STATUS()");

    // then
    assertThatResponseMap(procedureResponse)
        .hasOKResponseCode()
        .hasField("status", "STARTED")
        .hasField("configurationStatus", "FINALIZED");

    // and
    assertInternalStatus(PAUSING, FINALIZED);
  }

  @Test
  void shouldReturnPausedStateFromGetConnectorStatusInsteadOfStarting() {
    // given
    setConnectorStatus(STARTING, FINALIZED);

    // when
    var procedureResponse = callProcedure("GET_CONNECTOR_STATUS()");

    // then
    assertThatResponseMap(procedureResponse)
        .hasOKResponseCode()
        .hasField("status", "PAUSED")
        .hasField("configurationStatus", "FINALIZED");

    // and
    assertInternalStatus(STARTING, FINALIZED);
  }
}
