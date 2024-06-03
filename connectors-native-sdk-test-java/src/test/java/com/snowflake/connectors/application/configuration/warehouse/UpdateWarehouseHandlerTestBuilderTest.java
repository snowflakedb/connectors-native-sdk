/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import org.junit.jupiter.api.Test;

public class UpdateWarehouseHandlerTestBuilderTest {

  @Test
  void shouldInitializeUpdateWarehouseHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new UpdateWarehouseHandlerTestBuilder()
                    .withInputValidator(mock(UpdateWarehouseInputValidator.class))
                    .withCallback(mock(UpdateWarehouseCallback.class))
                    .withSdkCallback(mock(UpdateWarehouseSdkCallback.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withConnectorStatusService(mock(ConnectorStatusService.class))
                    .withConnectorConfigurationService(mock(ConnectorConfigurationService.class))
                    .build());
  }
}
