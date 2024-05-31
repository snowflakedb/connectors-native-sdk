/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.prerequisite;

import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.PREREQUISITES_DONE;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

import com.snowflake.connectors.util.ConnectorStatus;
import com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CompletePrerequisitesStepIntegrationTest extends BasePrerequisiteTest {

  @Test
  void shouldCompletePrerequisitesAndChangeConnectorStatusWhenConfigurationStatusIsInstalled() {
    // given
    setConnectorStatus(CONFIGURING, INSTALLED);

    // when
    var result = callProcedure("COMPLETE_PREREQUISITES_STEP()");

    // then
    assertThat(result).hasOkResponseCode().hasMessage("Prerequisites step completed successfully.");
    assertExternalStatus(CONFIGURING, PREREQUISITES_DONE);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      mode = EXCLUDE,
      names = {"INSTALLED", "FINALIZED"})
  @DisplayName(
      "should return success response and do not switch connector configuration status to"
          + " PREREQUISITES_DONE when configurationsStatus is not INSTALLED nor FINALIZED")
  void shouldCompletePrerequisitesButNotChangeConnectorStatus(
      ConnectorConfigurationStatus configurationStatus) {
    // given
    setConnectorStatus(CONFIGURING, configurationStatus);

    // when
    var result = callProcedure("COMPLETE_PREREQUISITES_STEP()");

    // then
    assertThat(result).hasOkResponseCode().hasMessage("Prerequisites step completed successfully.");
    assertExternalStatus(CONFIGURING, configurationStatus);
  }

  @Test
  void shouldReturnInvalidConnectorConfigurationStatusError_WhenConnectorConfigStatusIsFinalized() {
    // given
    setConnectorStatus(CONFIGURING, FINALIZED);

    // when
    var result = callProcedure("COMPLETE_PREREQUISITES_STEP()");

    // then:
    assertThat(result)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            "Invalid connector configuration status. Expected one of statuses: [INSTALLED,"
                + " PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: FINALIZED.");
  }

  @ParameterizedTest
  @EnumSource(value = ConnectorStatus.class, mode = EXCLUDE, names = "CONFIGURING")
  void shouldReturnInvalidConnectorStatusError_WhenConnectorStatusIsDifferentFromConfiguring(
      ConnectorStatus connectorStatus) {
    // given
    setConnectorStatus(connectorStatus, CONFIGURED);

    // when
    var result = callProcedure("COMPLETE_PREREQUISITES_STEP()");

    // then
    String expectedErrorMessage =
        String.format(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: %s.",
            connectorStatus);
    assertThat(result).hasResponseCode("INVALID_CONNECTOR_STATUS").hasMessage(expectedErrorMessage);
  }
}
