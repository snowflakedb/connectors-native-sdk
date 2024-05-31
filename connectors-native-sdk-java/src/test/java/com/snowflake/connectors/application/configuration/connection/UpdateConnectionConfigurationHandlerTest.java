/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.exception.InvalidConnectorStatusException;
import com.snowflake.connectors.common.exception.InMemoryConnectorErrorHelper;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class UpdateConnectionConfigurationHandlerTest {

  private static final FullConnectorStatus INITIAL_STATE =
      new FullConnectorStatus(PAUSED, FINALIZED);

  private static final Map<String, String> CONNECTION_CONFIG =
      Map.of(
          "key1", "\"val1\".\"val1.5\"",
          "key2", "val2",
          "key3", "val3");

  private final ConnectionConfigurationService configurationService =
      new DefaultConnectionConfigurationService(
          new DefaultConfigurationRepository(new InMemoryDefaultKeyValueTable()));

  private final ConnectorStatusService statusService =
      new DefaultConnectorStatusService(
          ConnectorStatusRepository.getInstance(new InMemoryDefaultKeyValueTable()));

  private final ConnectorErrorHelper errorHelper = new InMemoryConnectorErrorHelper();

  @Test
  void shouldUpdateConfig() {
    // given
    configurationService.updateConfiguration(new Variant("oldConfig"));
    statusService.updateConnectorStatus(INITIAL_STATE);
    var handler = initializeBuilder().build();

    // when
    var response = handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG));

    // then
    assertThat(response).hasOKResponseCode();
    assertThat(configurationService.getConfiguration()).isEqualTo(new Variant(CONNECTION_CONFIG));
    assertThat(statusService.getConnectorStatus()).isEqualTo(INITIAL_STATE);
  }

  @Test
  void shouldReturnErrorWhenStatusIsNotPaused() {
    // given
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));
    var handler = initializeBuilder().build();

    // then
    assertThatExceptionOfType(InvalidConnectorStatusException.class)
        .isThrownBy(() -> handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG)))
        .withMessage(
            "Invalid connector status. Expected status: [PAUSED]. Current status:" + " STARTED.");
  }

  @Test
  void shouldNotUpdateConfigWhenInputValidationFailed() {
    // given
    var oldConfig = new Variant("oldConfig");
    configurationService.updateConfiguration(oldConfig);
    statusService.updateConnectorStatus(INITIAL_STATE);
    var handler =
        initializeBuilder()
            .withInputValidator(
                it -> ConnectorResponse.error("INPUT_VALIDATION_FAILED", "Input validation failed"))
            .build();

    // when
    var response = handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG));

    // then
    assertThat(response)
        .hasResponseCode("INPUT_VALIDATION_FAILED")
        .hasMessage("Input validation failed");
    assertThat(configurationService.getConfiguration()).isEqualTo(oldConfig);
    assertThat(statusService.getConnectorStatus()).isEqualTo(INITIAL_STATE);
  }

  @Test
  void shouldNotUpdateConfigWhenDraftDraftCallbackFailed() {
    // given
    var oldConfig = new Variant("oldConfig");
    configurationService.updateConfiguration(oldConfig);
    statusService.updateConnectorStatus(INITIAL_STATE);
    var handler =
        initializeBuilder()
            .withDraftCallback(
                it -> ConnectorResponse.error("DRAFT_CALLBACK_FAILED", "Draft callback failed"))
            .build();

    // when
    var response = handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG));

    // then
    assertThat(response)
        .hasResponseCode("DRAFT_CALLBACK_FAILED")
        .hasMessage("Draft callback failed");
    assertThat(configurationService.getConfiguration()).isEqualTo(oldConfig);
    assertThat(statusService.getConnectorStatus()).isEqualTo(INITIAL_STATE);
  }

  @Test
  void shouldNotUpdateConfigWhenDraftConnectionValidatorFailed() {
    // given
    var oldConfig = new Variant("oldConfig");
    configurationService.updateConfiguration(oldConfig);
    statusService.updateConnectorStatus(INITIAL_STATE);
    var handler =
        initializeBuilder()
            .withDraftConnectionValidator(
                it -> ConnectorResponse.error("DRAFT_VALIDATION_FAILED", "Draft validation failed"))
            .build();

    // when
    var response = handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG));

    // then
    assertThat(response)
        .hasResponseCode("DRAFT_VALIDATION_FAILED")
        .hasMessage("Draft validation failed");
    assertThat(configurationService.getConfiguration()).isEqualTo(oldConfig);
    assertThat(statusService.getConnectorStatus()).isEqualTo(INITIAL_STATE);
  }

  @Test
  void shouldUpdateConfigWhenCallbackFailed() {
    // given
    var oldConfig = new Variant("oldConfig");
    configurationService.updateConfiguration(oldConfig);
    statusService.updateConnectorStatus(INITIAL_STATE);
    var handler =
        initializeBuilder()
            .withCallback(it -> ConnectorResponse.error("CALLBACK_FAILED", "Callback failed"))
            .build();

    // when
    var response = handler.updateConnectionConfiguration(new Variant(CONNECTION_CONFIG));

    // then
    assertThat(response).hasResponseCode("CALLBACK_FAILED").hasMessage("Callback failed");
    assertThat(configurationService.getConfiguration()).isEqualTo(new Variant(CONNECTION_CONFIG));
    assertThat(statusService.getConnectorStatus()).isEqualTo(INITIAL_STATE);
  }

  private UpdateConnectionConfigurationHandlerTestBuilder initializeBuilder() {
    return new UpdateConnectionConfigurationHandlerTestBuilder()
        .withInputValidator(config -> ConnectorResponse.success())
        .withDraftCallback(config -> ConnectorResponse.success())
        .withDraftConnectionValidator(config -> ConnectorResponse.success())
        .withCallback(config -> ConnectorResponse.success())
        .withConnectionValidator(ConnectorResponse::success)
        .withErrorHelper(errorHelper)
        .withConnectionConfigurationService(configurationService)
        .withConnectorStatusService(statusService);
  }
}
