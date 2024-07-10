/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.TASK_PROPERTIES;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.configuration.connector.InMemoryConnectorConfigurationService;
import com.snowflake.connectors.application.scheduler.CreateSchedulerHandlerTestBuilder;
import com.snowflake.connectors.application.scheduler.InMemoryDefaultSchedulerCreator;
import com.snowflake.connectors.application.scheduler.SchedulerCreator;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.InMemoryConnectorStatusRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.state.DefaultKeyValueStateRepository;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.taskreactor.InMemoryTaskManagement;
import com.snowflake.connectors.util.snowflake.InMemoryAccessTools;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class UpdateWarehouseHandlerTest {

  private static final String ALTERNATIVE_WAREHOUSE_NAME = "XS";
  private static final String WAREHOUSE_NAME = "TEST_WH";

  private InMemoryAccessTools accessTools;
  private InMemoryTaskManagement taskManagement;
  private UpdateWarehouseSdkCallback sdkCallback;
  private ConnectorStatusService statusService;
  private ConnectorConfigurationService configurationService;
  private SchedulerCreator schedulerCreator;

  @BeforeEach
  public void setUp() {
    var keyValueTable = new InMemoryDefaultKeyValueTable();
    var keyValueStateRepository =
        new DefaultKeyValueStateRepository<>(keyValueTable, FullConnectorStatus.class);
    var statusRepository = new InMemoryConnectorStatusRepository(keyValueStateRepository);
    var configurationRepository = new DefaultConfigurationRepository(keyValueTable);
    this.accessTools = new InMemoryAccessTools();
    this.taskManagement = new InMemoryTaskManagement();
    this.configurationService = new InMemoryConnectorConfigurationService(configurationRepository);
    this.sdkCallback =
        new DefaultUpdateWarehouseSdkCallback(taskManagement, taskManagement, mock());
    this.statusService = new DefaultConnectorStatusService(statusRepository);
    this.schedulerCreator =
        new InMemoryDefaultSchedulerCreator(configurationService, taskManagement, taskManagement);
  }

  @Test
  void shouldUpdateWarehouseInConfigAndTask() {
    // given
    var handler = initializeUpdateWarehouseHandlerBuilder().build();
    configureConnector();
    createScheduler();
    pauseConnector();
    accessTools.addWarehouses(WAREHOUSE_NAME);

    // when
    handler.updateWarehouse(WAREHOUSE_NAME);

    // then
    var status = statusService.getConnectorStatus();
    assertConnectorConfigSavedWarehouse(WAREHOUSE_NAME);
    assertSchedulerTaskWarehouse(WAREHOUSE_NAME);
    assertThat(status).isInStatus(PAUSED).isInConfigurationStatus(FINALIZED);
  }

  @Test
  void shouldReturnErrorIfConnectorIsNotPaused() {
    // given
    configureConnector();
    var handler = initializeUpdateWarehouseHandlerBuilder().build();

    // expect
    assertThatThrownBy(() -> handler.updateWarehouse(WAREHOUSE_NAME))
        .hasMessageContaining(
            "Invalid connector status. Expected status: [PAUSED]. Current status:" + " STARTED.");
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "")
  void shouldReturnErrorIfWarehouseNameEmptyOrNull(String warehouse) {
    // given
    pauseConnector();
    var handler = initializeUpdateWarehouseHandlerBuilder().build();

    // expect
    assertThatThrownBy(() -> handler.updateWarehouse(warehouse))
        .hasMessageContaining(format("'%s' is not a valid Snowflake identifier", warehouse));
  }

  @Test
  void shouldReturnErrorIfWarehouseAlreadyUsed() {
    // given
    configureConnector();
    pauseConnector();
    var handler = initializeUpdateWarehouseHandlerBuilder().build();

    // expect
    assertThatThrownBy(() -> handler.updateWarehouse(ALTERNATIVE_WAREHOUSE_NAME))
        .hasMessageContaining(
            format("Warehouse %s is already used by the application", ALTERNATIVE_WAREHOUSE_NAME));
  }

  @Test
  void shouldReturnErrorIfApplicationHasNoAccessToWarehouse() {
    // given
    configureConnector();
    pauseConnector();
    var handler = initializeUpdateWarehouseHandlerBuilder().build();

    // expect
    assertThatThrownBy(() -> handler.updateWarehouse(WAREHOUSE_NAME))
        .hasMessageContaining(
            format("Warehouse %s is inaccessible to the application", WAREHOUSE_NAME));
  }

  private void configureConnector() {
    var config =
        new Variant(
            Map.of(
                "warehouse",
                ALTERNATIVE_WAREHOUSE_NAME,
                "global_schedule",
                Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * * *")));
    configurationService.updateConfiguration(config);
    assertConnectorConfigSavedWarehouse(ALTERNATIVE_WAREHOUSE_NAME);
    statusService.updateConnectorStatus(new FullConnectorStatus(STARTED, FINALIZED));
  }

  private void pauseConnector() {
    statusService.updateConnectorStatus(new FullConnectorStatus(PAUSED, FINALIZED));
  }

  private void createScheduler() {

    var handler = initializeCreateSchedulerHandlerBuilder().build();
    var response = handler.createScheduler();
    assertThat(response).hasOKResponseCode();
    assertSchedulerTaskWarehouse("reference('WAREHOUSE_REFERENCE')");
  }

  private void assertSchedulerTaskWarehouse(String warehouse) {
    var task = taskManagement.showTask(SCHEDULER_TASK);
    assertThat(task).isNotEmpty().get(TASK_PROPERTIES).hasWarehouse(warehouse);
  }

  private void assertConnectorConfigSavedWarehouse(String warehouse) {
    var savedConfig = configurationService.getConfiguration();
    JsonAssertions.assertThatJson(savedConfig.asJsonString())
        .inPath("warehouse")
        .isEqualTo(warehouse);
  }

  private UpdateWarehouseHandlerTestBuilder initializeUpdateWarehouseHandlerBuilder() {
    return new UpdateWarehouseHandlerTestBuilder()
        .withInputValidator(
            new DefaultUpdateWarehouseInputValidator(accessTools, configurationService))
        .withCallback(validation -> ConnectorResponse.success())
        .withSdkCallback(sdkCallback)
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withConnectorStatusService(statusService)
        .withConnectorConfigurationService(configurationService);
  }

  private CreateSchedulerHandlerTestBuilder initializeCreateSchedulerHandlerBuilder() {
    return new CreateSchedulerHandlerTestBuilder()
        .withSchedulerCreator(schedulerCreator)
        .withErrorHelper(ConnectorErrorHelper.builder(null, "RESOURCE").build());
  }
}
