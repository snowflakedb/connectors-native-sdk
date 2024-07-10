/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.lifecycle;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import org.junit.jupiter.api.Test;

public class PauseConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldPauseConnector() {
    // given
    setConnectorStatus(STARTED, FINALIZED);

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertExternalStatus(PAUSED, FINALIZED);
  }
}
