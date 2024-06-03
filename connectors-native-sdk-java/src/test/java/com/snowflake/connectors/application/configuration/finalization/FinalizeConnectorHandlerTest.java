/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import org.junit.jupiter.api.Test;

public class FinalizeConnectorHandlerTest {

  private static final FullConnectorStatus CONNECTED_STATUS =
      new FullConnectorStatus(CONFIGURING, CONNECTED);
  private static final Variant FINALIZE_CONFIG = new Variant("");

  private final InMemoryDefaultKeyValueTable connectorStatusTable =
      new InMemoryDefaultKeyValueTable();
  private final ConnectorStatusService statusService =
      new DefaultConnectorStatusService(
          ConnectorStatusRepository.getInstance(connectorStatusTable));

  @Test
  void shouldNotUpdateStatusWhenSdkCallbackFails() {
    // given
    String errorCode = "ERROR";
    String errorMessage = "Error response message";
    var handler =
        initializeBuilder()
            .withSdkCallback(() -> ConnectorResponse.error(errorCode, errorMessage))
            .build();
    statusService.updateConnectorStatus(CONNECTED_STATUS);

    // when
    var response = handler.finalizeConnectorConfiguration(FINALIZE_CONFIG);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONNECTED);
  }

  private FinalizeConnectorHandlerTestBuilder initializeBuilder() {
    return new FinalizeConnectorHandlerTestBuilder()
        .withConnectorStatusService(statusService)
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withInputValidator(input -> ConnectorResponse.success())
        .withSourceValidator(input -> ConnectorResponse.success())
        .withCallback(input -> ConnectorResponse.success())
        .withSdkCallback(ConnectorResponse::success);
  }
}
