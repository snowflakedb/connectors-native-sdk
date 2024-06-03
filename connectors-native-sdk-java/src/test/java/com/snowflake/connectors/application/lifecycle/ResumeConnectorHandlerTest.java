/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import com.snowflake.connectors.application.lifecycle.resume.ResumeConnectorHandlerTestBuilder;
import com.snowflake.connectors.common.exception.InMemoryConnectorErrorHelper;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.taskreactor.InstanceStreamService;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class ResumeConnectorHandlerTest {

  private final LifecycleService lifecycleService = mock();
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor = mock();
  private final ConnectorErrorHelper connectorErrorHelper = new InMemoryConnectorErrorHelper();
  private final InstanceStreamService instanceStreamService = mock();
  private final ResumeTaskReactorService resumeTaskReactorService = mock();
  @Captor private ArgumentCaptor<Consumer<Identifier>> recreateStreamsLambdaCaptor;

  @BeforeEach
  void setUp() {
    openMocks(this);
  }

  @Test
  void shouldRecreateInstanceStreams() {
    // given
    var resumeConnectorHandler = initializeBuilder().build();
    Identifier exampleInstance = Identifier.from("EXAMPLE_INSTANCE");

    // when
    resumeConnectorHandler.resumeConnector();

    // then
    verify(taskReactorInstanceActionExecutor)
        .applyToAllExistingTaskReactorInstances(recreateStreamsLambdaCaptor.capture());
    recreateStreamsLambdaCaptor.getValue().accept(exampleInstance);
    verify(instanceStreamService, times(1)).recreateStreams(exampleInstance);
  }

  private ResumeConnectorHandlerTestBuilder initializeBuilder() {
    when(lifecycleService.withRollbackHandling(any())).thenReturn(ConnectorResponse.success());
    return new ResumeConnectorHandlerTestBuilder()
        .withStateValidator(ConnectorResponse::success)
        .withCallback(ConnectorResponse::success)
        .withResumeConnectorSdkCallback(ConnectorResponse::success)
        .withTaskReactorInstanceActionExecutor(taskReactorInstanceActionExecutor)
        .withErrorHelper(connectorErrorHelper)
        .withInstanceStreamService(instanceStreamService)
        .withLifecycleService(lifecycleService)
        .withResumeTaskReactorService(resumeTaskReactorService);
  }
}
