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

import com.snowflake.connectors.application.lifecycle.pause.PauseConnectorHandlerTestBuilder;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.exception.InvalidConnectorStatusException;
import com.snowflake.connectors.common.exception.InMemoryConnectorErrorHelper;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.connectors.util.snowflake.PrivilegeTools;
import com.snowflake.connectors.util.snowflake.RequiredPrivilegesMissingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class PauseConnectorHandlerTest {

  private final PrivilegeTools privilegeTools = mock();
  private final PauseTaskReactorService pauseTaskReactorService = mock();

  private final InMemoryDefaultKeyValueTable connectorStatusTable =
      new InMemoryDefaultKeyValueTable();
  private final ConnectorStatusService statusService =
      new DefaultConnectorStatusService(
          ConnectorStatusRepository.getInstance(connectorStatusTable));

  private final ConnectorErrorHelper errorHelper = new InMemoryConnectorErrorHelper();
  private final LifecycleService lifecycleService =
      new DefaultLifecycleService(privilegeTools, statusService, STARTED);

  @AfterEach
  void clear() {
    connectorStatusTable.clear();
  }

  @Test
  void shouldPauseConnector() {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

    // then
    assertThat(handlerResponse).hasOKResponseCode();
    assertThat(statusService.getConnectorStatus())
        .isInStatus(PAUSED)
        .isInConfigurationStatus(FINALIZED);

    verify(pauseTaskReactorService, times(1)).pauseAllInstances();
  }

  @Test
  void shouldReturnErrorIfExecuteTaskPrivilegeIsNotGranted() {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    doThrow(new RequiredPrivilegesMissingException("EXECUTE TASK"))
        .when(privilegeTools)
        .validatePrivileges(any());

    // expect
    assertThatThrownBy(handler::pauseConnector)
        .isInstanceOf(RequiredPrivilegesMissingException.class)
        .hasMessage(
            "To perform this operation the application must be granted the following privileges:"
                + " EXECUTE TASK");

    assertThat(statusService.getConnectorStatus())
        .isInStatus(STARTED)
        .isInConfigurationStatus(FINALIZED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"STARTED", "PAUSING"})
  void shouldReturnErrorIfConnectorNotInStartedOrPausingState(ConnectorStatus currentStatus) {
    // given
    var handler = initBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(currentStatus, FINALIZED));

    // expect
    assertThatThrownBy(handler::pauseConnector)
        .isInstanceOf(InvalidConnectorStatusException.class)
        .hasMessage(
            format(
                "Invalid connector status. Expected status: [STARTED, PAUSING]. Current status:"
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
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ERROR").hasMessage("Error response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(STARTED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldRollbackChangesIfCallbackReturnedRollbackCode() {
    // given
    var handler =
        initBuilder()
            .withCallback(() -> ConnectorResponse.error("ROLLBACK", "Rollback response message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(STARTED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldRollbackChangesIfSdkCallbackReturnedRollbackCode() {
    // given
    var handler =
        initBuilder()
            .withSdkCallback(() -> ConnectorResponse.error("ROLLBACK", "Rollback response message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

    // then
    assertThat(handlerResponse).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertThat(statusService.getConnectorStatus())
        .isInStatus(STARTED)
        .isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldSetErrorStateIfCallbackDidNotReturnOkOrRollback() {
    // given
    var handler =
        initBuilder()
            .withSdkCallback(() -> ConnectorResponse.error("SOME_ERROR", "Some error message"))
            .build();
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

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
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));

    // when
    var handlerResponse = handler.pauseConnector();

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

  private PauseConnectorHandlerTestBuilder initBuilder() {
    return new PauseConnectorHandlerTestBuilder()
        .withStateValidator(ConnectorResponse::success)
        .withCallback(ConnectorResponse::success)
        .withSdkCallback(ConnectorResponse::success)
        .withErrorHelper(errorHelper)
        .withLifecycleService(lifecycleService)
        .withPauseTaskReactorService(pauseTaskReactorService);
  }
}
