/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.application.scheduler.SchedulerManager;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import org.junit.jupiter.api.Test;

class PauseConnectorHandlerTestBuilderTest {

  @Test
  void shouldInitializePauseConnectorHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new PauseConnectorHandlerTestBuilder()
                    .withStateValidator(mock(PauseConnectorStateValidator.class))
                    .withCallback(mock(PauseConnectorCallback.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withLifecycleService(mock(LifecycleService.class))
                    .withSdkCallback(mock(PauseConnectorSdkCallback.class))
                    .withPauseTaskReactorService(mock(PauseTaskReactorService.class))
                    .withSchedulerManager(mock(SchedulerManager.class))
                    .build());
  }
}
