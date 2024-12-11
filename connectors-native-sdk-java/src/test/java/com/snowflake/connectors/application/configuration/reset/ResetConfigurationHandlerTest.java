/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationService;
import com.snowflake.connectors.application.configuration.connection.DefaultConnectionConfigurationService;
import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.configuration.connector.InMemoryConnectorConfigurationService;
import com.snowflake.connectors.application.configuration.prerequisites.PrerequisitesRepository;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.InMemoryConnectorStatusRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.state.DefaultKeyValueStateRepository;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ResetConfigurationHandlerTest {

  private static final Variant connectorConfig = new Variant(Map.of("warehouse", "w1"));
  private static final Variant connectionConfig = new Variant(Map.of("key1", "val1"));

  private ConnectorStatusService connectorStatusService;
  private ConfigurationRepository configurationRepository;
  private PrerequisitesRepository prerequisitesRepository;
  private ConnectorConfigurationService configurationService;
  private ConnectionConfigurationService connectionConfigurationService;
  private ResetConfigurationSdkCallback sdkCallback;

  @BeforeEach
  public void setUp() {
    var keyValueStateRepository =
        new DefaultKeyValueStateRepository<>(
            new InMemoryDefaultKeyValueTable(), FullConnectorStatus.class);
    var statusRepository = new InMemoryConnectorStatusRepository(keyValueStateRepository);
    this.connectorStatusService = new DefaultConnectorStatusService(statusRepository);
    this.configurationRepository =
        new DefaultConfigurationRepository(new InMemoryDefaultKeyValueTable());
    this.prerequisitesRepository = mock(PrerequisitesRepository.class);
    this.configurationService = new InMemoryConnectorConfigurationService(configurationRepository);
    this.connectionConfigurationService =
        new DefaultConnectionConfigurationService(configurationRepository);
    this.sdkCallback =
        new DefaultResetConfigurationSdkCallback(
            configurationRepository, prerequisitesRepository, new InMemoryTransactionManager());
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.ConnectorConfigurationStatus.class,
      names = {"INSTALLED", "PREREQUISITES_DONE", "CONFIGURED", "CONNECTED"})
  void shouldResetConfigurationAndUpdateStatus(
      ConnectorStatus.ConnectorConfigurationStatus currentConfigurationStatus) {
    // given
    var handler = initializeResetConfigurationHandlerBuilder().build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(CONFIGURING, currentConfigurationStatus));

    // when
    var response = handler.resetConfiguration();

    // then
    var status = connectorStatusService.getConnectorStatus();
    assertThat(response).hasOKResponseCode();
    verify(prerequisitesRepository).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsEmpty();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(INSTALLED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR", "STARTING", "PAUSING"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    var handler = initializeResetConfigurationHandlerBuilder().build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(
        new FullConnectorStatus(currentStatus, CONFIGURED));

    // when + then
    assertThatThrownBy(handler::resetConfiguration)
        .hasMessageContaining(
            "Invalid connector status. Expected status: [CONFIGURING]. Current status: "
                + currentStatus
                + ".");
    verify(prerequisitesRepository, never()).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsSaved();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(currentStatus).isInConfigurationStatus(CONFIGURED);
  }

  @Test
  void shouldThrowExceptionWhenConnectorConfigurationStatusIsFinalized() {
    // given
    var handler = initializeResetConfigurationHandlerBuilder().build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, FINALIZED));

    // when + then
    assertThatThrownBy(handler::resetConfiguration)
        .hasMessageContaining(
            "Invalid connector configuration status. Expected one of statuses: [INSTALLED,"
                + " PREREQUISITES_DONE, CONFIGURED, CONNECTED]. Current status: FINALIZED.");
    verify(prerequisitesRepository, never()).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsSaved();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldReturnErrorWhenValidatorResponseIsNotOk() {
    // given
    var handler =
        initializeResetConfigurationHandlerBuilder()
            .withValidator(() -> ConnectorResponse.error("UNKNOWN_ERROR", "Error message"))
            .build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, CONNECTED));

    // when
    var response = handler.resetConfiguration();

    // then
    assertThat(response.getResponseCode()).isEqualTo("UNKNOWN_ERROR");
    assertThat(response.getMessage()).isEqualTo("Error message");
    verify(prerequisitesRepository, never()).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsSaved();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(CONNECTED);
  }

  @Test
  void shouldReturnErrorWhenSdkCallbackResponseIsNotOk() {
    // given
    var handler =
        initializeResetConfigurationHandlerBuilder()
            .withSdkCallback(() -> ConnectorResponse.error("UNKNOWN_ERROR", "Error message"))
            .build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, CONNECTED));

    // when
    var response = handler.resetConfiguration();

    // then
    assertThat(response.getResponseCode()).isEqualTo("UNKNOWN_ERROR");
    assertThat(response.getMessage()).isEqualTo("Error message");
    verify(prerequisitesRepository, never()).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsSaved();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(CONNECTED);
  }

  @Test
  void shouldReturnErrorWhenCallbackResponseIsNotOk() {
    // given
    var handler =
        initializeResetConfigurationHandlerBuilder()
            .withCallback(() -> ConnectorResponse.error("UNKNOWN_ERROR", "Error message"))
            .build();
    insertConfiguration();
    connectorStatusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, CONNECTED));

    // when
    var response = handler.resetConfiguration();

    // then
    assertThat(response.getResponseCode()).isEqualTo("UNKNOWN_ERROR");
    assertThat(response.getMessage()).isEqualTo("Error message");
    verify(prerequisitesRepository, never()).markAllPrerequisitesAsUndone();
    assertConnectorConfigIsSaved();
    var status = connectorStatusService.getConnectorStatus();
    assertThat(status).isInStatus(CONFIGURING).isInConfigurationStatus(CONNECTED);
  }

  private ResetConfigurationHandlerTestBuilder initializeResetConfigurationHandlerBuilder() {
    return new ResetConfigurationHandlerTestBuilder()
        .withValidator(ConnectorResponse::success)
        .withSdkCallback(sdkCallback)
        .withCallback(ConnectorResponse::success)
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withConnectorStatusService(connectorStatusService);
  }

  private void insertConfiguration() {
    configurationService.updateConfiguration(connectorConfig);
    connectionConfigurationService.updateConfiguration(connectionConfig);
  }

  private void assertConnectorConfigIsEmpty() {
    var savedConfig = configurationRepository.fetchAll();
    assertThat(savedConfig.size()).isEqualTo(0);
  }

  private void assertConnectorConfigIsSaved() {
    assertThat(configurationService.getConfiguration()).isEqualTo(connectorConfig);
    assertThat(connectionConfigurationService.getConfiguration()).isEqualTo(connectionConfig);
  }
}
