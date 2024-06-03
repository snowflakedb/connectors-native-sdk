/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.lifecycle;

import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ERROR;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSING;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.ConnectorStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class PauseConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldPauseConnector() {
    // given
    setConnectorStatus(STARTED, FINALIZED);

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response).hasOkResponseCode();
    assertExternalStatus(PAUSED, FINALIZED);
  }

  @Test
  void shouldReturnStartedStateFromGetConnectorStatusInsteadOfPausing() {
    // given
    setConnectorStatus(PAUSING, FINALIZED);

    // expect
    assertExternalStatus(STARTED, FINALIZED);
    assertInternalStatus(PAUSING, FINALIZED);
  }

  @Test
  void shouldReturnErrorIfExecuteTaskPrivilegeIsNotGranted() {
    // given
    setConnectorStatus(STARTED, FINALIZED);
    application.revokeExecuteTaskPrivilege();

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("REQUIRED_PRIVILEGES_MISSING")
        .hasMessage(
            "To perform this operation the application must be granted the following privileges: "
                + "EXECUTE TASK");
    assertInternalStatus(STARTED, FINALIZED);

    // cleanup
    application.grantExecuteTaskPrivilege();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"STARTED", "PAUSING"})
  void shouldReturnErrorIfConnectorNotInStartedState(ConnectorStatus currentStatus) {
    // given
    setConnectorStatus(currentStatus, FINALIZED);

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [STARTED, PAUSING]. Current status: "
                + currentStatus.name()
                + ".");
    assertInternalStatus(currentStatus, FINALIZED);
  }

  @Test
  void shouldReturnErrorIfValidationDidNotReturnOk() {
    // given
    setConnectorStatus(STARTED, FINALIZED);
    mockProcedure("PAUSE_CONNECTOR_VALIDATE()", "ERROR", "Error response message");

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response).hasResponseCode("ERROR").hasMessage("Error response message");
    assertExternalStatus(STARTED, FINALIZED);

    // cleanup
    mockProcedure("PAUSE_CONNECTOR_VALIDATE()", "OK", null);
  }

  @Test
  void shouldRollbackChangesIfInternalCallbackReturnedRollbackCode() {
    // given
    setConnectorStatus(STARTED, FINALIZED);
    mockProcedure("PAUSE_CONNECTOR_INTERNAL()", "ROLLBACK", "Rollback response message");

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertExternalStatus(STARTED, FINALIZED);

    // cleanup
    mockProcedure("PAUSE_CONNECTOR_INTERNAL()", "OK", null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"OBJECT_CONSTRUCT(\\'a\\', \\'b\\')", "NULL"})
  void shouldSetErrorStateIfInternalCallbackDidNotReturnOkOrRollback(String callbackResponse) {
    // given
    setConnectorStatus(STARTED, FINALIZED);
    mockProcedureWithBody("PAUSE_CONNECTOR_INTERNAL()", "RETURN " + callbackResponse);

    // when
    var response = callProcedure("PAUSE_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("UNKNOWN_ERROR")
        .hasMessage(
            "An unknown error occurred and the connector is now in an unspecified state. "
                + "Contact your connector provider, manual intervention may be required.");
    assertExternalStatus(ERROR, FINALIZED);

    // cleanup
    mockProcedure("PAUSE_CONNECTOR_INTERNAL()", "OK", null);
  }
}
