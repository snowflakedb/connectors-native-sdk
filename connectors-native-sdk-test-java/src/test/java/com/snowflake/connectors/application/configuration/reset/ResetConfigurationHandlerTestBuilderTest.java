/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import org.junit.jupiter.api.Test;

class ResetConfigurationHandlerTestBuilderTest {

  @Test
  void shouldInitializeResetConfigurationHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new ResetConfigurationHandlerTestBuilder()
                    .withValidator(mock(ResetConfigurationValidator.class))
                    .withSdkCallback(mock(ResetConfigurationSdkCallback.class))
                    .withCallback(mock(ResetConfigurationCallback.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withConnectorStatusService(mock(ConnectorStatusService.class))
                    .build());
  }
}
