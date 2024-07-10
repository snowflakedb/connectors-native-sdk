/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle;

import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ERROR;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.snowflake.connectors.application.lifecycle.resume.ResumeConnectorHandlerTestBuilder;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.exception.InvalidConnectorStatusException;
import com.snowflake.connectors.common.exception.InMemoryConnectorErrorHelper;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.taskreactor.InstanceStreamService;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import com.snowflake.connectors.util.snowflake.PrivilegeTools;
import com.snowflake.connectors.util.snowflake.RequiredPrivilegesMissingException;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ResumeConnectorHandlerTest {

  @Captor private ArgumentCaptor<Consumer<Identifier>> recreateStreamsLambdaCaptor;

  private final PrivilegeTools privilegeTools = mock();
  private final InstanceStreamService instanceStreamService = mock();
  private final ResumeTaskReactorService resumeTaskReactorService = mock();
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor = mock();

  private final InMemoryDefaultKeyValueTable connectorStatusTable =
      new InMemoryDefaultKeyValueTable();
  private final ConnectorStatusService statusService =
      new DefaultConnectorStatusService(
          ConnectorStatusRepository.getInstance(connectorStatusTable));

  private final ConnectorErrorHelper errorHelper = new InMemoryConnectorErrorHelper();
  private final LifecycleService lifecycleService =
      new DefaultLifecycleService(privilegeTools, statusService, PAUSED);

  private final Identifier exampleTaskReactorInstance = Identifier.from("EXAMPLE_INSTANCE");

  @AfterEach
  void clear() {
    connectorStatusTable.clear();
  }

  @Test
  void shouldResumeConnector() {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse).hasOKResponseCode();
    assertThat(statusService.getConnectorStatus())
        .isInStatus(STARTED)
        .isInConfigurationStatus(FINALIZED);

    verify(resumeTaskReactorService, times(1)).resumeAllInstances();
    verify(taskReactorInstanceActionExecutor)
        .applyToAllInitializedTaskReactorInstances(recreateStreamsLambdaCaptor.capture());
    recreateStreamsLambdaCaptor.getValue().accept(exampleTaskReactorInstance);
    verify(instanceStreamService, times(1)).recreateStreams(exampleTaskReactorInstance);
  }

  @Test
  void shouldReturnErrorIfExecuteTaskPrivilegeIsNotGranted() {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    doThrow(new RequiredPrivilegesMissingException("EXECUTE TASK"))
        .when(privilegeTools)
        .validatePrivileges(any());

    // expect
    assertThatThrownBy(handler::resumeConnector)
        .isInstanceOf(RequiredPrivilegesMissingException.class)
        .hasMessage(
            "To perform this operation the application must be granted the following privileges:"
                + " EXECUTE TASK");

    assertThat(statusService.getConnectorStatus())
        .isInStatus(PAUSED)
        .isInConfigurationStatus(FINALIZED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"PAUSED", "STARTING"})
  void shouldReturnErrorIfConnectorNotInPausedOrStartingState(ConnectorStatus currentStatus) {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(currentStatus, FINALIZED));

    // expect
    assertThatThrownBy(handler::resumeConnector)
        .isInstanceOf(InvalidConnectorStatusException.class)
        .hasMessage(
            format(
                "Invalid connector status. Expected status: [PAUSED, STARTING]. Current status:"
                    + " %s.",
                currentStatus));

    assertThat(statusService.getConnectorStatus())
        .isInStatus(currentStatus)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldReturnErrorIfValidationDidNotReturnOk() {
    // given
    var handler =
        initBuilder()
            .withStateValidator(() -> ConnectorResponse.error("ERROR", "Error response message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ERROR").hasMessage("Error response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(PAUSED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldRollbackChangesIfCallbackReturnedRollbackCode() {
    // given
    var handler =
        initBuilder()
            .withCallback(() -> ConnectorResponse.error("ROLLBACK", "Rollback response message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(PAUSED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldRollbackChangesIfSdkCallbackReturnedRollbackCode() {
    // given
    var handler =
        initBuilder()
            .withSdkCallback(() -> ConnectorResponse.error("ROLLBACK", "Rollback response message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(PAUSED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldSetErrorStateIfCallbackDidNotReturnOkOrRollback() {
    // given
    var handler =
        initBuilder()
            .withSdkCallback(() -> ConnectorResponse.error("SOME_ERROR", "Some error message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse)
        .hasResponseCode("UNKNOWN_ERROR")
        .hasMessage(
            "An unknown error occurred and the connector is now in an unspecified state. Contact"
                + " your connector provider, manual intervention may be required.");

    assertThat(statusService.getConnectorStatus())
        .isInStatus(ERROR)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldSetErrorStateIfCallbackThrewException() {
    // given
    var handler =
        initBuilder()
            .withSdkCallback(
                () -> {
                  throw new NullPointerException("Some error message");
                })
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));

    // when
    var handlerResponse = handler.resumeConnector();

    // then
    assertThat(handlerResponse)
        .hasResponseCode("UNKNOWN_ERROR")
        .hasMessage(
            "An unknown error occurred and the connector is now in an unspecified state. Contact"
                + " your connector provider, manual intervention may be required.");

    assertThat(statusService.getConnectorStatus())
        .isInStatus(ERROR)
        .isInConfigurationStatus(FINALIZED);
  }

  private ResumeConnectorHandlerTestBuilder initBuilder() {
    return new ResumeConnectorHandlerTestBuilder()
        .withStateValidator(ConnectorResponse::success)
        .withCallback(ConnectorResponse::success)
        .withSdkCallback(ConnectorResponse::success)
        .withTaskReactorInstanceActionExecutor(taskReactorInstanceActionExecutor)
        .withErrorHelper(errorHelper)
        .withInstanceStreamService(instanceStreamService)
        .withLifecycleService(lifecycleService)
        .withResumeTaskReactorService(resumeTaskReactorService);
  }
}
