/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.prerequisite;

import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static com.snowflake.connectors.util.RowUtil.row;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

import com.snowflake.connectors.util.ConnectorStatus;
import com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class UpdatePrerequisitesIntegrationTest extends BasePrerequisiteTest {

  @ParameterizedTest
  @EnumSource(value = ConnectorConfigurationStatus.class, mode = EXCLUDE, names = "FINALIZED")
  @DisplayName(
      "UPDATE_PREREQUISITE should update successfully chosen PREREQUISITE when it exists and when"
          + " connector status is CONFIGURING and configuration status is not FINALIZED")
  void shouldUpdatePrerequisite(ConnectorConfigurationStatus configurationStatus) {
    // given
    setConnectorStatus(CONFIGURING, configurationStatus);

    // when
    var response = callProcedure("UPDATE_PREREQUISITE('2', TRUE)");

    // then
    assertThat(response).hasOkResponseCode().hasMessage("Prerequisite updated successfully.");
    assertPrerequisitesTableContent(row("1", false), row("2", true), row("3", false));

    // cleanup
    markAllPrerequisitesAsUndone();
  }

  @ParameterizedTest
  @EnumSource(value = ConnectorStatus.class, mode = EXCLUDE, names = "CONFIGURING")
  void updatePrerequisiteShouldThrowInvalidConnectorStatusError_WhenConnectorStatusIsNotConfiguring(
      ConnectorStatus connectorStatus) {
    // given
    setConnectorStatus(connectorStatus, INSTALLED);

    // when
    var response = callProcedure("UPDATE_PREREQUISITE('2', TRUE)");

    // then
    String expectedErrorMessage =
        String.format(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: %s.",
            connectorStatus);
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(expectedErrorMessage);
  }

  @Test
  void
      updatePrerequisitesShouldThrowInvalidConnectorConfigurationStatusError_WhenConfigurationStatusIsFinalized() {
    // given
    setConnectorStatus(CONFIGURING, FINALIZED);

    // when
    var response = callProcedure("UPDATE_PREREQUISITE('2', TRUE)");

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            "Invalid connector configuration status. Expected one of statuses: [INSTALLED,"
                + " PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: FINALIZED.");
  }

  @Test
  void
      updatePrerequisiteShouldThrowPrerequisiteNotFoundError_WhenPrerequisiteWithGivenIfDoesNotExist() {
    // given
    setConnectorStatus(CONFIGURING, CONFIGURED);

    // when
    var response = callProcedure("UPDATE_PREREQUISITE('9', TRUE)");

    // then
    assertThat(response)
        .hasResponseCode("PREREQUISITE_NOT_FOUND")
        .hasMessage("Prerequisite with ID: 9 not found.");
  }

  @ParameterizedTest
  @EnumSource(value = ConnectorConfigurationStatus.class, mode = EXCLUDE, names = "FINALIZED")
  @DisplayName(
      "MARK_ALL_PREREQUISITES_AS_DONE should set is_completed status to TRUE for all prerequisites"
          + " when connector status is CONFIGURING and configuration status is not FINALIZED")
  void shouldMarkAllAsDone(ConnectorConfigurationStatus configurationStatus) {
    // given
    setConnectorStatus(CONFIGURING, configurationStatus);

    // when
    var response = callProcedure("MARK_ALL_PREREQUISITES_AS_DONE()");

    // then
    assertThat(response)
        .hasOkResponseCode()
        .hasMessage("All prerequisites have been marked as done.");
    assertPrerequisitesTableContent(row("1", true), row("2", true), row("3", true));

    // cleanup
    markAllPrerequisitesAsUndone();
  }

  @ParameterizedTest
  @EnumSource(value = ConnectorStatus.class, mode = EXCLUDE, names = "CONFIGURING")
  @DisplayName(
      "MARK_ALL_PREREQUISITES_AS_DONE should throw INVALID_CONNECTOR_STATUS exception when"
          + " connector status is not CONFIGURING")
  void shouldNotMarkAndThrowInvalidConnectorStatus(ConnectorStatus connectorStatus) {
    // given
    setConnectorStatus(connectorStatus, INSTALLED);

    // when
    var response = callProcedure("MARK_ALL_PREREQUISITES_AS_DONE()");

    // then
    String expectedErrorMessage =
        String.format(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: %s.",
            connectorStatus);
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(expectedErrorMessage);
  }

  @Test
  void
      markAllPrerequisitesAsDoneShouldThrowInvalidConnectorConfigurationStatusError_WhenConfigurationStatusIsFinalized() {
    // given
    setConnectorStatus(CONFIGURING, FINALIZED);

    // when
    var response = callProcedure("MARK_ALL_PREREQUISITES_AS_DONE()");

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            "Invalid connector configuration status. Expected one of statuses: [INSTALLED,"
                + " PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: FINALIZED.");
  }
}
