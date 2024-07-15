/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.finalization;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.InMemoryConnectorStatusRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.state.DefaultKeyValueStateRepository;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class FinalizeConnectorHandlerTest {

  private static final Variant customConfig = new Variant(Map.of("journal_table", "j_table_name"));

  private ConfigurationRepository configurationRepository;
  private ConnectorStatusService connectorStatusService;

  @BeforeEach
  void setUp() {
    var keyValueTable = new InMemoryDefaultKeyValueTable();
    var keyValueStateRepository =
        new DefaultKeyValueStateRepository<>(keyValueTable, FullConnectorStatus.class);
    var connectorStatusRepository = new InMemoryConnectorStatusRepository(keyValueStateRepository);
    this.configurationRepository = new DefaultConfigurationRepository(keyValueTable);
    this.connectorStatusService = new DefaultConnectorStatusService(connectorStatusRepository);
  }

  @Test
  void shouldChangeStatusToStartedAndFinalized() {
    // given
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, CONNECTED));
    var handler = initializeBuilder().withConnectorStatusService(connectorStatusService).build();

    // when
    var response = handler.finalizeConnectorConfiguration(customConfig);

    // then
    assertThat(response).hasOKResponseCode();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(STARTED).isInConfigurationStatus(FINALIZED);
    assertSavedConfiguration(customConfig);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(currentStatus, CONNECTED));
    var handler = initializeBuilder().withConnectorStatusService(connectorStatusService).build();

    // when
    var response = handler.finalizeConnectorConfiguration(customConfig);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: "
                + currentStatus.name()
                + ".");
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(currentStatus).isInConfigurationStatus(CONNECTED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = "CONNECTED")
  void shouldReturnErrorWhenConfigurationStatusNotConnected(
      ConnectorConfigurationStatus currentStatus) {
    // given
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, currentStatus));
    var handler = initializeBuilder().withConnectorStatusService(connectorStatusService).build();

    // when
    var response = handler.finalizeConnectorConfiguration(customConfig);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            "Invalid connector configuration status. Expected one of statuses: [CONNECTED]. "
                + "Current status: "
                + currentStatus.name()
                + ".");
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(currentStatus);
  }

  @ParameterizedTest
  @MethodSource("provideErrors")
  void shouldReturnErrorWhenHandlerStepFails(
      ConnectorResponse validateResponse,
      ConnectorResponse validateSourceResponse,
      ConnectorResponse internalResponse) {
    // given
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, CONNECTED));
    var handler =
        initializeBuilder()
            .withConnectorStatusService(connectorStatusService)
            .withInputValidator(validator -> validateResponse)
            .withSourceValidator(validator -> validateSourceResponse)
            .withCallback(callback -> internalResponse)
            .build();

    // when
    var response = handler.finalizeConnectorConfiguration(customConfig);

    // then
    assertThat(response).hasResponseCode("ERROR").hasMessage("Error response message");
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(CONNECTED);
    assertConfigurationNotSaved();
  }

  private static List<Arguments> provideErrors() {
    return List.of(
        Arguments.of(
            ConnectorResponse.error("ERROR", "Error response message"),
            ConnectorResponse.success(),
            ConnectorResponse.success()),
        Arguments.of(
            ConnectorResponse.success(),
            ConnectorResponse.error("ERROR", "Error response message"),
            ConnectorResponse.success()),
        Arguments.of(
            ConnectorResponse.success(),
            ConnectorResponse.success(),
            ConnectorResponse.error("ERROR", "Error response message")));
  }

  private void assertSavedConfiguration(Variant expectedConfig) {
    var savedConfig =
        configurationRepository.fetch("custom_configuration", Variant.class).orElse(null);
    assertThat(savedConfig).isEqualTo(expectedConfig);
  }

  private void assertConfigurationNotSaved() {
    var savedConfig = configurationRepository.fetch("custom_configuration", Variant.class);
    assertThat(savedConfig).isEmpty();
  }

  private FinalizeConnectorHandlerTestBuilder initializeBuilder() {
    return new FinalizeConnectorHandlerTestBuilder()
        .withInputValidator(validation -> ConnectorResponse.success())
        .withSourceValidator(validation -> ConnectorResponse.success())
        .withCallback(
            callback -> {
              configurationRepository.update("custom_configuration", customConfig);
              return ConnectorResponse.success();
            })
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withConnectorStatusService(connectorStatusService)
        .withSdkCallback(ConnectorResponse::success);
  }
}
