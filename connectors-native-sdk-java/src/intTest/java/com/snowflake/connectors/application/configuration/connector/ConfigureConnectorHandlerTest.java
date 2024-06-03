/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DATA_OWNER_ROLE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.GLOBAL_SCHEDULE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.OPERATIONAL_WAREHOUSE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.WAREHOUSE;
import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.PREREQUISITES_DONE;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.response.ConnectorResponse.error;
import static com.snowflake.connectors.common.response.ConnectorResponse.success;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ConfigureConnectorHandlerTest extends BaseIntegrationTest {
  private static final Variant CONFIG =
      new Variant(
          Map.of(
              WAREHOUSE.getPropertyName(),
              "warehouse",
              DESTINATION_DATABASE.getPropertyName(),
              "destination_db",
              DESTINATION_SCHEMA.getPropertyName(),
              "destination_schema",
              DATA_OWNER_ROLE.getPropertyName(),
              "role",
              OPERATIONAL_WAREHOUSE.getPropertyName(),
              "operational_warehouse",
              GLOBAL_SCHEDULE.getPropertyName(),
              new Variant(Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *"))));

  ConfigureConnectorHandler configureConnectorHandler =
      ConfigureConnectorHandler.builder(session)
          .withInputValidator(validation -> success())
          .withCallback(callback -> success())
          .build();

  @BeforeEach
  public void cleanupConfig() {
    session.sql("DELETE FROM STATE.APP_CONFIG WHERE KEY = 'connector_configuration'").collect();
  }

  @Test
  public void shouldSaveConfigurationAndChangeStatusToCONFIGURED() {
    // given
    connectorIsInPrerequisitesDoneStatus();

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(CONFIG);

    // then
    assertThat(response).hasOKResponseCode().hasMessage("Connector successfully configured.");
    assertThat(getConnectorStatus()).isInStatus(CONFIGURING).isInConfigurationStatus(CONFIGURED);
    assertThat(getConfiguration()).isEqualTo(CONFIG);
  }

  @Test
  public void shouldReturnErrorWhenConfigContainsInvalidFields() {
    // given
    connectorIsInPrerequisitesDoneStatus();
    Variant config =
        new Variant(
            Map.of(
                WAREHOUSE.getPropertyName(),
                "warehouse",
                DESTINATION_DATABASE.getPropertyName(),
                "destination_db",
                DESTINATION_SCHEMA.getPropertyName(),
                "destination_schema",
                DATA_OWNER_ROLE.getPropertyName(),
                "role",
                OPERATIONAL_WAREHOUSE.getPropertyName(),
                "operational_warehouse",
                "invalid_field_1",
                "value",
                "invalid_field_2",
                "value2"));

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(config);

    // then
    assertThat(response)
        .hasResponseCode("CONNECTOR_CONFIGURATION_PARSING_ERROR")
        .hasMessage("Fields invalid_field_1, invalid_field_2 are not allowed in configuration.");
  }

  @Test
  public void
      shouldReturnErrorNotSaveConfigurationAndNotChangeConnectorStatusWhenInputValidatorReturnsError() {
    // given
    String validationErrorCode = "CUSTOM_CODE";
    String validationErrorMessage = "custom message";

    // and
    ConfigureConnectorHandler configureConnectorHandler =
        ConfigureConnectorHandler.builder(session)
            .withInputValidator(validation -> error(validationErrorCode, validationErrorMessage))
            .withCallback(callback -> success())
            .build();

    // and
    connectorIsInPrerequisitesDoneStatus();

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(CONFIG);

    // then
    assertThat(response).hasResponseCode(validationErrorCode).hasMessage(validationErrorMessage);
    assertThatExceptionOfType(ConnectorConfigurationNotFoundException.class)
        .isThrownBy(this::getConfiguration)
        .withMessage("Connector configuration record not found in database.");
    assertThat(getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(PREREQUISITES_DONE);
  }

  @Test
  public void
      shouldReturnError_SaveConfigurationAndNotChangeConnectorStatus_whenInternalHandlerReturnsError() {
    // given
    String internalErrorCode = "CUSTOM_CODE";
    String internalErrorMessage = "custom message";

    // and
    ConfigureConnectorHandler configureConnectorHandler =
        ConfigureConnectorHandler.builder(session)
            .withInputValidator(validation -> success())
            .withCallback(callback -> error(internalErrorCode, internalErrorMessage))
            .build();
    // and
    connectorIsInPrerequisitesDoneStatus();

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(CONFIG);

    // then
    assertThat(response).hasResponseCode(internalErrorCode).hasMessage(internalErrorMessage);
    assertThat(getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(PREREQUISITES_DONE);
    assertThat(getConfiguration()).isEqualTo(CONFIG);
  }

  @Test
  public void shouldReturnCONNECTOR_CONFIGURATION_PARSING_ERROR() {
    // given
    Variant configuration = new Variant("wrong_value");
    String expectedErrorMessage = "Given configuration is not a valid json";

    // and
    connectorIsInPrerequisitesDoneStatus();

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(configuration);

    // then
    assertThat(response).hasResponseCode("INTERNAL_ERROR").hasMessage(expectedErrorMessage);
  }

  @ParameterizedTest(name = "{index} => message=''{0}''")
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED"})
  public void shouldReturnINVALID_CONNECTOR_STATUS(ConnectorStatus status) {
    // given
    String expectedErrorCode = "INVALID_CONNECTOR_STATUS";
    String expectedErrorMessage =
        "Invalid connector status. Expected status: [CONFIGURING]. Current status: " + status + ".";

    // and
    connectorIsInStatus(status);

    // when
    ConnectorResponse response = configureConnectorHandler.configureConnector(CONFIG);

    // then
    assertThat(response).hasResponseCode(expectedErrorCode).hasMessage(expectedErrorMessage);
  }

  void connectorIsInPrerequisitesDoneStatus() {
    ConnectorStatusService connectorStatusService = ConnectorStatusService.getInstance(session);
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, PREREQUISITES_DONE));
  }

  FullConnectorStatus getConnectorStatus() {
    ConnectorStatusService connectorStatusService = ConnectorStatusService.getInstance(session);
    return connectorStatusService.getConnectorStatus();
  }

  Variant getConfiguration() {
    ConnectorConfigurationService configurationService =
        ConnectorConfigurationService.getInstance(session);
    return configurationService.getConfiguration();
  }

  void connectorIsInStatus(ConnectorStatus status) {
    ConnectorStatusService connectorStatusService = ConnectorStatusService.getInstance(session);
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(status, PREREQUISITES_DONE));
  }
}
