/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.PREREQUISITES_DONE;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.ConnectorStatus;
import com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigureConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  private final String connectorConfig =
      "{"
          + "\"warehouse\": \"xs\", "
          + "\"destination_database\": \"dest_db\", "
          + "\"destination_schema\": \"dest_schema\", "
          + "\"data_owner_role\": \"role\""
          + "}";

  @ParameterizedTest
  @MethodSource("provideCorrectStatuses")
  void shouldSaveConfigurationAndChangeConfigurationStatusToConfiguredIfItIsNotFinalized(
      ConnectorConfigurationStatus currentStatus, ConnectorConfigurationStatus expectedStatus) {
    // given
    setConnectorStatus(CONFIGURING, currentStatus);

    // when
    var response = configureConnector();

    // then
    assertThat(response).hasOkResponseCode();
    assertExternalStatus(CONFIGURING, expectedStatus);
    assertSavedConfiguration();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    setConnectorStatus(currentStatus, INSTALLED);

    // when
    var response = configureConnector();

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [CONFIGURING]. "
                + "Current status: "
                + currentStatus.name()
                + ".");
    assertExternalStatus(currentStatus, INSTALLED);
  }

  @Test
  void shouldPersistStatusAndConfigOnUpgrade() {
    // given
    setConnectorStatus(CONFIGURING, CONFIGURED);
    configureConnector();
    var updatedAt = getUpdatedAt();

    // when
    application.upgrade();

    // then
    assertExternalStatus(CONFIGURING, CONFIGURED);
    assertThat(getUpdatedAt()).isEqualTo(updatedAt);
    assertExactlyOneConnectorConfiguration();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      names = {"CONNECTED", "FINALIZED"})
  void shouldNotUpdateStatusesWhenLaterWizardStepAlreadyDone(
      ConnectorConfigurationStatus currentStatus) {
    // given
    setConnectorStatus(CONFIGURING, currentStatus);

    // when
    var response = configureConnector();

    // then
    assertThat(response).hasOkResponseCode();
    assertExternalStatus(CONFIGURING, currentStatus);
  }

  private Map<String, Variant> configureConnector() {
    return callProcedure(format("CONFIGURE_CONNECTOR(PARSE_JSON('%s'))", connectorConfig));
  }

  private Timestamp getUpdatedAt() {
    return executeInApp("SELECT updated_at FROM STATE.APP_CONFIG")[0].getTimestamp(0);
  }

  private void assertSavedConfiguration() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).isEqualTo(connectorConfig);
  }

  private void assertExactlyOneConnectorConfiguration() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    assertThat(executeInApp(query).length).isEqualTo(1);
  }

  private Set<Arguments> provideCorrectStatuses() {
    return Set.of(
        Arguments.of(INSTALLED, CONFIGURED),
        Arguments.of(PREREQUISITES_DONE, CONFIGURED),
        Arguments.of(CONFIGURED, CONFIGURED),
        Arguments.of(CONNECTED, CONNECTED));
  }
}
