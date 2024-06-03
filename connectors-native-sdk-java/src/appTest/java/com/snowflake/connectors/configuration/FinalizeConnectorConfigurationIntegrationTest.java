/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.ConnectorStatus;
import com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class FinalizeConnectorConfigurationIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final String customConfig = "{\"journal_table\": \"j_table_name\"}";

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();
    mockProcedureWithBody(
        "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL(config VARIANT)",
        "INSERT INTO STATE.APP_CONFIG SELECT \\'custom_configuration\\', :config, "
            + "current_timestamp(); RETURN OBJECT_CONSTRUCT(\\'response_code\\', \\'OK\\')");
  }

  @Test
  void shouldChangeStatusToStartedAndFinalized() {
    // given
    setConnectorStatus(CONFIGURING, CONNECTED);

    // when
    var response = finalizeConfiguration();

    // then
    assertThat(response).hasOkResponseCode();
    assertExternalStatus(STARTED, FINALIZED);
    assertSavedConfiguration();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    setConnectorStatus(currentStatus, CONNECTED);

    // when
    var response = finalizeConfiguration();

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: "
                + currentStatus.name()
                + ".");
    assertExternalStatus(currentStatus, CONNECTED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = "CONNECTED")
  void shouldReturnErrorWhenConfigurationStatusNotConnected(
      ConnectorConfigurationStatus currentStatus) {
    // given
    setConnectorStatus(CONFIGURING, currentStatus);

    // when
    var response = finalizeConfiguration();

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            "Invalid connector configuration status. Expected one of statuses: [CONNECTED]. "
                + "Current status: "
                + currentStatus.name()
                + ".");
    assertExternalStatus(CONFIGURING, currentStatus);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "FINALIZE_CONNECTOR_CONFIGURATION_VALIDATE(config VARIANT)",
        "VALIDATE_SOURCE(config VARIANT)",
        "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL(config VARIANT)"
      })
  void shouldReturnErrorWhenHandlerStepFails(String procedure) {
    // given
    setConnectorStatus(CONFIGURING, CONNECTED);
    mockProcedure(procedure, "ERROR", "Error response message");

    // when
    var response = finalizeConfiguration();

    // then
    assertThat(response).hasResponseCode("ERROR").hasMessage("Error response message");
    assertExternalStatus(CONFIGURING, CONNECTED);
    assertConfigurationNotSaved();

    // cleanup
    mockProcedure(procedure, "OK", null);
  }

  // Temporarily ignored due to a bug: JS function property SHOW_LANGUAGE_ERROR cannot be false
  @ValueSource(
      strings = {
        "FINALIZE_CONNECTOR_CONFIGURATION_VALIDATE",
        "VALIDATE_SOURCE",
        "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL"
      })
  void shouldReturnErrorWhenHandlerStepSqlProcedureThrowsException(String procedure) {
    // given
    setConnectorStatus(CONFIGURING, CONNECTED);
    mockProcedureToThrow(procedure + "(config VARIANT)");

    // when
    var response = finalizeConfiguration();

    // then
    assertThat(response)
        .hasResponseCode("UNKNOWN_SQL_ERROR")
        .hasMessage(
            "Unknown error occurred when calling "
                + procedure
                + " procedure: Uncaught exception "
                + "of type 'MOCK_EXCEPTION' on line \\d+ at position \\d+");
    assertExternalStatus(CONFIGURING, CONNECTED);
    assertConfigurationNotSaved();

    // cleanup
    mockProcedure(procedure + "(config VARIANT)", "OK", null);
  }

  private Map<String, Variant> finalizeConfiguration() {
    var query = format("FINALIZE_CONNECTOR_CONFIGURATION(PARSE_JSON('%s'))", customConfig);
    return callProcedure(query);
  }

  private void assertSavedConfiguration() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'custom_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).isEqualTo(customConfig);
  }

  private void assertConfigurationNotSaved() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'custom_configuration'";
    Assertions.assertThat(executeInApp(query)).isEmpty();
  }
}
