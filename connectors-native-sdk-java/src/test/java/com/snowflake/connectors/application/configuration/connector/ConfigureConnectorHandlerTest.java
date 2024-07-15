/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static com.snowflake.connectors.application.configuration.connector.ConfigureConnectorHandler.ERROR_TYPE;
import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.PREREQUISITES_DONE;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.response.ConnectorResponse.error;
import static com.snowflake.connectors.common.response.ConnectorResponse.success;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.InMemoryConnectorStatusRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.state.DefaultKeyValueStateRepository;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigureConnectorHandlerTest {

  private static final Variant CONNECTOR_CONFIG =
      new Variant(
          "{"
              + "\"warehouse\": \"xs\", "
              + "\"destination_database\": \"dest_db\", "
              + "\"destination_schema\": \"dest_schema\", "
              + "\"data_owner_role\": \"role\""
              + "}");

  ConfigureConnectorHandlerBuilder configureConnectorHandlerBuilder;
  ConnectorStatusService connectorStatusService;
  ConfigurationRepository configurationRepository;
  ConnectorConfigurationService connectorConfigurationService;

  @BeforeEach
  void setUp() {
    var keyValueTable = new InMemoryDefaultKeyValueTable();
    var keyValueStateRepository =
        new DefaultKeyValueStateRepository<>(keyValueTable, FullConnectorStatus.class);
    var connectorStatusRepository = new InMemoryConnectorStatusRepository(keyValueStateRepository);

    this.configurationRepository = new DefaultConfigurationRepository(keyValueTable);
    this.connectorConfigurationService =
        new DefaultConnectorConfigurationService(configurationRepository);
    this.connectorStatusService = new DefaultConnectorStatusService(connectorStatusRepository);
    this.configureConnectorHandlerBuilder =
        new ConfigureConnectorHandlerTestBuilder()
            .withInputValidator(validation -> success())
            .withCallback(callback -> success())
            .withStatusService(connectorStatusService)
            .withConfigurationService(connectorConfigurationService)
            .withErrorHelper(ConnectorErrorHelper.builder(null, "RESOURCE").build());
  }

  @ParameterizedTest
  @MethodSource("provideCorrectStatuses")
  void shouldSaveConfigurationAndChangeConfigurationStatusToConfiguredIfItIsNotFinalized(
      ConnectorConfigurationStatus currentStatus, ConnectorConfigurationStatus expectedStatus) {
    // given
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, currentStatus));
    var handler = configureConnectorHandlerBuilder.build();

    // when
    var response = handler.configureConnector(CONNECTOR_CONFIG);

    // then
    var connectorStatus = connectorStatusService.getConnectorStatus();
    assertThat(response).hasOKResponseCode();
    assertThat(connectorStatus).isInStatus(CONFIGURING).isInConfigurationStatus(expectedStatus);
    assertSavedConfiguration();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(currentStatus, INSTALLED));
    var handler = configureConnectorHandlerBuilder.build();

    // when
    var response = handler.configureConnector(CONNECTOR_CONFIG);

    // then
    var connectorStatus = connectorStatusService.getConnectorStatus();
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [CONFIGURING]. "
                + "Current status: "
                + currentStatus.name()
                + ".");
    assertThat(connectorStatus).isInStatus(currentStatus).isInConfigurationStatus(INSTALLED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      names = {"CONNECTED", "FINALIZED"})
  void shouldNotUpdateStatusesWhenLaterWizardStepAlreadyDone(
      ConnectorConfigurationStatus currentStatus) {
    // given
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, currentStatus));
    var handler = configureConnectorHandlerBuilder.build();

    // when
    var response = handler.configureConnector(CONNECTOR_CONFIG);

    // then
    var connectorStatus = connectorStatusService.getConnectorStatus();
    assertThat(response).hasOKResponseCode();
    assertThat(connectorStatus).isInStatus(CONFIGURING).isInConfigurationStatus(currentStatus);
  }

  @Test
  void shouldReturnErrorOnCallback() {
    // given
    var errorMessage = String.format("Validation result with error: %s", ERROR_TYPE);
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, INSTALLED));
    configureConnectorHandlerBuilder.withCallback(validation -> error(ERROR_TYPE, errorMessage));
    var handler = configureConnectorHandlerBuilder.build();

    // when
    var response = handler.configureConnector(CONNECTOR_CONFIG);

    // then
    var connectorStatus = connectorStatusService.getConnectorStatus();
    assertThat(response).hasResponseCode(ERROR_TYPE).hasMessage(errorMessage);
    assertThat(connectorStatus).isInStatus(CONFIGURING);
    assertSavedConfiguration();
  }

  @Test
  void shouldReturnValidationResultWithError() {
    // given
    var errorMessage = String.format("Validation result with error: %s", ERROR_TYPE);
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, INSTALLED));
    configureConnectorHandlerBuilder.withInputValidator(
        validation -> error(ERROR_TYPE, errorMessage));
    var handler = configureConnectorHandlerBuilder.build();

    // when
    var response = handler.configureConnector(CONNECTOR_CONFIG);

    // then
    var connectorStatus = connectorStatusService.getConnectorStatus();
    assertThat(response).hasResponseCode(ERROR_TYPE).hasMessage(errorMessage);
    assertThat(connectorStatus).isInStatus(CONFIGURING);
    assertNotSavedConfiguration();
  }

  private void assertSavedConfiguration() {
    var savedConfig =
        configurationRepository.fetch("connector_configuration", Variant.class).orElse(null);
    assertThat(savedConfig).isEqualTo(CONNECTOR_CONFIG);
  }

  private void assertNotSavedConfiguration() {
    var savedConfig =
        configurationRepository.fetch("connector_configuration", Variant.class).orElse(null);
    assertThat(savedConfig).isEqualTo(null);
  }

  private static Set<Arguments> provideCorrectStatuses() {
    return Set.of(
        Arguments.of(INSTALLED, CONFIGURED),
        Arguments.of(PREREQUISITES_DONE, CONFIGURED),
        Arguments.of(CONFIGURED, CONFIGURED),
        Arguments.of(CONNECTED, CONNECTED));
  }
}
