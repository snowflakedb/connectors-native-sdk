/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import org.junit.jupiter.api.Test;

public class ConnectionConfigurationHandlerTestBuilderTest {

  @Test
  void shouldInitializeConnectionConfigurationHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new ConnectionConfigurationHandlerTestBuilder()
                    .withInputValidator(mock(ConnectionConfigurationInputValidator.class))
                    .withCallback(mock(ConnectionConfigurationCallback.class))
                    .withConnectionValidator(mock(ConnectionValidator.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withConnectionConfigurationService(mock(ConnectionConfigurationService.class))
                    .withConnectorStatusService(mock(ConnectorStatusService.class))
                    .build());
  }
}
