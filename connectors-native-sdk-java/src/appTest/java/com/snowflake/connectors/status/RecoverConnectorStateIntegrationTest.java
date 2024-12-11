/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.status;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ERROR;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSING;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTING;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.ConnectorStatus;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class RecoverConnectorStateIntegrationTest extends BaseNativeSdkIntegrationTest {

  @ParameterizedTest
  @MethodSource("validStateTransitions")
  void shouldForceConnectorStateUpdate(ConnectorStatus currentStatus, ConnectorStatus newStatus) {
    // given
    setConnectorStatus(currentStatus, FINALIZED);

    // when
    var response =
        callProcedure(format("RECOVER_CONNECTOR_STATE(%s)", asVarchar(newStatus.name())));

    // then
    assertThatResponseMap(response)
        .hasOKResponseCode()
        .hasMessage(format("Connector status successfully changed to %s", newStatus));
    assertInternalStatus(newStatus, FINALIZED);
  }

  @Test
  void shouldReturnErrorIfStatusRecordMissing() {
    // given
    executeInApp("DELETE FROM STATE.APP_STATE WHERE key = 'connector_status'");

    // when
    var response = callProcedure("RECOVER_CONNECTOR_STATE('PAUSED')");

    // then
    assertThatResponseMap(response)
        .hasResponseCode("CONNECTOR_STATUS_NOT_FOUND")
        .hasMessage("Connector status data does not exist in the database");
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"CONFIGURING", "STARTED", "PAUSED"})
  void shouldReturnErrorIfCurrentStateIsInvalid(ConnectorStatus currentStatus) {
    // given
    setConnectorStatus(currentStatus, FINALIZED);

    // when
    var response = callProcedure("RECOVER_CONNECTOR_STATE('PAUSED')");

    // then
    assertThatResponseMap(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            format(
                "Invalid connector status. Expected status: [ERROR, STARTING, PAUSING]. Current"
                    + " status: %s",
                currentStatus));
    assertInternalStatus(currentStatus, FINALIZED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"CONFIGURING", "STARTING", "PAUSING", "ERROR"})
  void shouldReturnErrorIfNewStateIsInvalid(ConnectorStatus newStatus) {
    // given
    setConnectorStatus(ERROR, FINALIZED);

    // when
    var response =
        callProcedure(format("RECOVER_CONNECTOR_STATE(%s)", asVarchar(newStatus.name())));

    // then
    assertThatResponseMap(response)
        .hasResponseCode("INVALID_NEW_CONNECTOR_STATUS")
        .hasMessage(
            format(
                "Invalid new connector status. Expected status: [STARTED, PAUSED]. New status: %s",
                newStatus));
    assertInternalStatus(ERROR, FINALIZED);
  }

  private Stream<Arguments> validStateTransitions() {
    return Stream.of(
        Arguments.of(ERROR, PAUSED),
        Arguments.of(ERROR, STARTED),
        Arguments.of(PAUSING, PAUSED),
        Arguments.of(PAUSING, STARTED),
        Arguments.of(STARTING, PAUSED),
        Arguments.of(STARTING, STARTED));
  }
}
