/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import org.junit.jupiter.api.Test;

public class FinalizeConnectorHandlerTestBuilderTest {

  @Test
  void shouldInitializeFinalizeConnectorHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new FinalizeConnectorHandlerTestBuilder()
                    .withInputValidator(mock(FinalizeConnectorInputValidator.class))
                    .withSourceValidator(mock(SourceValidator.class))
                    .withCallback(mock(FinalizeConnectorCallback.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withConnectorStatusService(mock(ConnectorStatusService.class))
                    .withSdkCallback(mock(FinalizeConnectorSdkCallback.class))
                    .build());
  }
}
