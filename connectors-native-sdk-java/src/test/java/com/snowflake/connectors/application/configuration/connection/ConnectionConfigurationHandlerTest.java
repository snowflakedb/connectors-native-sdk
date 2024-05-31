/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ConnectionConfigurationHandlerTest {

  private static final FullConnectorStatus CONFIGURED_STATUS =
      new FullConnectorStatus(CONFIGURING, CONFIGURED);

  private static final Variant CONNECTION_CONFIG =
      new Variant(
          Map.of(
              "key1", "\"val1\".\"val1.5\"",
              "key2", "val2",
              "key3", "val3"));

  private final InMemoryDefaultKeyValueTable connectorStatusTable =
      new InMemoryDefaultKeyValueTable();
  private final InMemoryDefaultKeyValueTable configurationTable =
      new InMemoryDefaultKeyValueTable();

  private final ConnectorStatusService statusService =
      new DefaultConnectorStatusService(
          ConnectorStatusRepository.getInstance(connectorStatusTable));
  private final ConnectionConfigurationService connectionConfigurationService =
      new DefaultConnectionConfigurationService(
          new DefaultConfigurationRepository(configurationTable));

  @AfterEach
  void clear() {
    connectorStatusTable.clear();
    configurationTable.clear();
  }

  @Test
  void shouldReturnOkCommitNewConfigAndUpdateStatusToConnected() {
    // given
    var handler = initializeBuilder().build();
    statusService.updateConnectorStatus(CONFIGURED_STATUS);

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response).hasOKResponseCode();
    assertThat(connectionConfigurationService.getConfiguration()).isEqualTo(CONNECTION_CONFIG);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONNECTED);
  }

  @Test
  void shouldPersistConfigTransformedByValidateProcedure() {
    // given
    var transformedConfig =
        new Variant(
            Map.of(
                "key_x", new Variant("value_x"),
                "key_y", new Variant("value_y")));
    var additionalPayload = Map.of("config", transformedConfig);

    var handler =
        initializeBuilder()
            .withInputValidator(input -> ConnectorResponse.success("msg", additionalPayload))
            .build();
    statusService.updateConnectorStatus(CONFIGURED_STATUS);

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response).hasOKResponseCode();
    assertThat(connectionConfigurationService.getConfiguration()).isEqualTo(transformedConfig);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONNECTED);
  }

  @Test
  void shouldReturnErrorResponseFromTestConnectionButKeepProvidedConnectionConfig() {
    // given
    String errorCode = "ERROR";
    String errorMessage = "Error response message";
    var handler =
        initializeBuilder()
            .withConnectionValidator(() -> ConnectorResponse.error(errorCode, errorMessage))
            .build();
    statusService.updateConnectorStatus(CONFIGURED_STATUS);

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONFIGURED);
    assertThat(connectionConfigurationService.getConfiguration()).isEqualTo(CONNECTION_CONFIG);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorStatus.class,
      names = {"STARTED", "PAUSED", "ERROR"})
  void shouldReturnErrorWhenConnectorStatusIsNotConfiguring(ConnectorStatus currentStatus) {
    // given
    var handler = initializeBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(currentStatus, CONFIGURED));

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_STATUS")
        .hasMessage(
            format(
                "Invalid connector status. Expected status: [CONFIGURING]. Current status: %s.",
                currentStatus.name()));
    assertThat(statusService.getConnectorStatus())
        .isInStatus(currentStatus)
        .isInConfigurationStatus(CONFIGURED);
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectorConfigurationStatus.class,
      names = {"INSTALLED", "PREREQUISITES_DONE"})
  void shouldReturnErrorWhenConfigurationStatusBeforeConfigured(
      ConnectorConfigurationStatus currentStatus) {
    // given
    var handler = initializeBuilder().build();
    statusService.updateConnectorStatus(new FullConnectorStatus(CONFIGURING, currentStatus));

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_CONNECTOR_CONFIGURATION_STATUS")
        .hasMessage(
            format(
                "Invalid connector configuration status. Expected one of statuses: [CONFIGURED,"
                    + " CONNECTED]. Current status: %s.",
                currentStatus.name()));
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(currentStatus);
  }

  @Test
  void shouldReturnErrorWhenInputValidatorFails() {
    // given
    String errorCode = "ERROR";
    String errorMessage = "Error response message";
    var handler =
        initializeBuilder()
            .withInputValidator(input -> ConnectorResponse.error(errorCode, errorMessage))
            .build();
    statusService.updateConnectorStatus(CONFIGURED_STATUS);

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONFIGURED);
  }

  @Test
  void shouldReturnErrorWhenCallbackFails() {
    // given
    String errorCode = "ERROR";
    String errorMessage = "Error response message";
    var handler =
        initializeBuilder()
            .withCallback(input -> ConnectorResponse.error(errorCode, errorMessage))
            .build();
    statusService.updateConnectorStatus(CONFIGURED_STATUS);

    // when
    var response = handler.setConnectionConfiguration(CONNECTION_CONFIG);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertThat(statusService.getConnectorStatus())
        .isInStatus(CONFIGURING)
        .isInConfigurationStatus(CONFIGURED);
  }

  private ConnectionConfigurationHandlerTestBuilder initializeBuilder() {
    return new ConnectionConfigurationHandlerTestBuilder()
        .withConnectionConfigurationService(connectionConfigurationService)
        .withConnectorStatusService(statusService)
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withConnectionValidator(ConnectorResponse::success)
        .withInputValidator(input -> ConnectorResponse.success())
        .withCallback(input -> ConnectorResponse.success());
  }
}
