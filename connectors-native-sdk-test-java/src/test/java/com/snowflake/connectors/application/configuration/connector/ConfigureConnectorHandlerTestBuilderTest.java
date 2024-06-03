/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import org.junit.jupiter.api.Test;

public class ConfigureConnectorHandlerTestBuilderTest {

  @Test
  void shouldInitializeConfigureConnectorHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new ConfigureConnectorHandlerTestBuilder()
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withInputValidator(mock(ConfigureConnectorInputValidator.class))
                    .withCallback(mock(ConfigureConnectorCallback.class))
                    .withConfigurationService(mock(ConnectorConfigurationService.class))
                    .withStatusService(mock(ConnectorStatusService.class))
                    .build());
  }
}
