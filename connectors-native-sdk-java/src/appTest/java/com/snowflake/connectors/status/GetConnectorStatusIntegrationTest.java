/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.status;

import static com.snowflake.connectors.util.ResponseAssertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import org.junit.jupiter.api.Test;

class GetConnectorStatusIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void
      shouldReturnConfiguringStatusAndInstalledConfigurationStatus_AfterApplicationIsInitialized() {
    var procedureResponse = callProcedure("GET_CONNECTOR_STATUS()");

    assertThat(procedureResponse)
        .hasOkResponseCode()
        .hasField("status", "CONFIGURING")
        .hasField("configurationStatus", "INSTALLED");
  }
}
