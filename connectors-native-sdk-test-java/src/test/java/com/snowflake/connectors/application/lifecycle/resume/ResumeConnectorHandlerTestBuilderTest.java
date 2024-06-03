/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import org.junit.jupiter.api.Test;

class ResumeConnectorHandlerTestBuilderTest {

  @Test
  void shouldInitializeResumeConnectorHandlerTestBuilder() {
    assertThatNoException()
        .isThrownBy(
            () ->
                new ResumeConnectorHandlerTestBuilder()
                    .withStateValidator(mock(ResumeConnectorStateValidator.class))
                    .withCallback(mock(ResumeConnectorCallback.class))
                    .withErrorHelper(mock(ConnectorErrorHelper.class))
                    .withLifecycleService(mock(LifecycleService.class))
                    .withResumeConnectorSdkCallback(mock(ResumeConnectorSdkCallback.class))
                    .withResumeTaskReactorService(mock(ResumeTaskReactorService.class))
                    .build());
  }
}
