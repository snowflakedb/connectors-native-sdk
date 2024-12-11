/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.DefaultConfigurationRepository;
import com.snowflake.connectors.application.configuration.connector.InMemoryConnectorConfigurationService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.table.InMemoryDefaultKeyValueTable;
import com.snowflake.connectors.taskreactor.InMemoryTaskManagement;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CreateSchedulerHandlerTest {

  private ConfigurationRepository configurationRepository;
  private CreateSchedulerHandler createSchedulerHandler;
  private InMemoryTaskManagement inMemoryTaskManagement;

  @BeforeEach
  void setUp() {
    var keyValueTable = new InMemoryDefaultKeyValueTable();
    this.configurationRepository = new DefaultConfigurationRepository(keyValueTable);

    var connectorConfigurationService =
        new InMemoryConnectorConfigurationService(configurationRepository);
    inMemoryTaskManagement = new InMemoryTaskManagement();
    var schedulerCreator =
        new DefaultSchedulerManager(
            connectorConfigurationService, inMemoryTaskManagement, inMemoryTaskManagement);
    this.createSchedulerHandler =
        new CreateSchedulerHandlerTestBuilder()
            .withSchedulerManager(schedulerCreator)
            .withErrorHelper(ConnectorErrorHelper.builder(null, "RESOURCE").build())
            .build();
  }

  @Test
  void shouldCreateScheduler() {
    // given
    setConnectorConfiguration();

    // when
    var procedureResponse = createSchedulerHandler.createScheduler();

    // then
    assertThat(procedureResponse).hasOKResponseCode();
    assertTaskWasCreated();
  }

  private void assertTaskWasCreated() {
    var schedulerTask = inMemoryTaskManagement.showTask(Scheduler.SCHEDULER_TASK);
    assertThat(schedulerTask).isPresent();
  }

  private void setConnectorConfiguration() {
    Variant config =
        new Variant(
            Map.of(
                "global_schedule",
                new Variant(Map.of("scheduleType", "CRON", "scheduleDefinition", "*/10 * * *"))));
    configurationRepository.update("connector_configuration", config);
  }
}
