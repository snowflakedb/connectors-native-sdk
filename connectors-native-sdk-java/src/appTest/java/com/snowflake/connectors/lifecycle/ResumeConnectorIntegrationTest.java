/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.lifecycle;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import org.junit.jupiter.api.Test;

public class ResumeConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldResumeConnector() {
    // given
    setConnectorStatus(PAUSED, FINALIZED);

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertExternalStatus(STARTED, FINALIZED);
  }
}
