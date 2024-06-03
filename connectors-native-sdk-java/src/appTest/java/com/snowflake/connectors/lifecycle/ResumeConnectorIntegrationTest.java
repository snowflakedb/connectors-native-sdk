/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.lifecycle;

import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ERROR;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTING;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.ConnectorStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ResumeConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldResumeConnector() {
    // given
    setConnectorStatus(PAUSED, FINALIZED);

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response).hasOkResponseCode();
    assertExternalStatus(STARTED, FINALIZED);
  }

  @Test
  void shouldReturnPausedStateFromGetConnectorStatusInsteadOfStarting() {
    // given
    setConnectorStatus(STARTING, FINALIZED);

    // expect
    assertExternalStatus(PAUSED, FINALIZED);
    assertInternalStatus(STARTING, FINALIZED);
  }

  @Test
  void shouldReturnErrorIfExecuteTaskPrivilegeIsNotGranted() {
    // given
    setConnectorStatus(PAUSED, FINALIZED);
    application.revokeExecuteTaskPrivilege();

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("REQUIRED_PRIVILEGES_MISSING")
        .hasMessage(
            "To perform this operation the application must be granted the following privileges: "
                + "EXECUTE TASK");
    assertInternalStatus(PAUSED, FINALIZED);

    // cleanup
    application.grantExecuteTaskPrivilege();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"PAUSED", "STARTING"})
  void shouldReturnErrorIfConnectorNotInPausedState(ConnectorStatus currentStatus) {
    // given
    setConnectorStatus(currentStatus, FINALIZED);

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [PAUSED, STARTING]. Current status: "
                + currentStatus.name()
                + ".");
    assertInternalStatus(currentStatus, FINALIZED);
  }

  @Test
  void shouldReturnErrorIfValidationDidNotReturnOk() {
    // given
    setConnectorStatus(PAUSED, FINALIZED);
    mockProcedure("RESUME_CONNECTOR_VALIDATE()", "ERROR", "Error response message");

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response).hasResponseCode("ERROR").hasMessage("Error response message");
    assertExternalStatus(PAUSED, FINALIZED);

    // cleanup
    mockProcedure("RESUME_CONNECTOR_VALIDATE()", "OK", null);
  }

  @Test
  void shouldRollbackChangesIfInternalCallbackReturnedRollbackCode() {
    // given
    setConnectorStatus(PAUSED, FINALIZED);
    mockProcedure("RESUME_CONNECTOR_INTERNAL()", "ROLLBACK", "Rollback response message");

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response).hasResponseCode("ROLLBACK").hasMessage("Rollback response message");
    assertExternalStatus(PAUSED, FINALIZED);

    // cleanup
    mockProcedure("RESUME_CONNECTOR_INTERNAL()", "OK", null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"OBJECT_CONSTRUCT(\\'a\\', \\'b\\')", "NULL"})
  void shouldSetErrorStateIfInternalCallbackDidNotReturnOkOrRollback(String callbackResponse) {
    // given
    setConnectorStatus(PAUSED, FINALIZED);
    mockProcedureWithBody("RESUME_CONNECTOR_INTERNAL()", "RETURN " + callbackResponse);

    // when
    var response = callProcedure("RESUME_CONNECTOR()");

    // then
    assertThat(response)
        .hasResponseCode("UNKNOWN_ERROR")
        .hasMessage(
            "An unknown error occurred and the connector is now in an unspecified state. "
                + "Contact your connector provider, manual intervention may be required.");
    assertExternalStatus(ERROR, FINALIZED);

    // cleanup
    mockProcedure("RESUME_CONNECTOR_INTERNAL()", "OK", null);
  }
}
