/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.TASK_PROPERTIES;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.common.assertions.NativeSdkAssertions;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskRef;
import java.io.IOException;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UpdateWarehouseTest extends BaseNativeSdkIntegrationTest {

  @BeforeAll
  @Override
  public void beforeAll() throws IOException {
    super.beforeAll();
    createWarehouse("TEST_WH");
  }

  @Test
  void shouldUpdateWarehouseInConfigAndTask() {
    // given
    configureConnector();
    setupWarehouseReference();
    createScheduler();
    pauseConnector();

    // when
    callProcedure("UPDATE_WAREHOUSE('TEST_WH')");

    // then
    assertConnectorConfigSavedWarehouse("TEST_WH");
    assertSchedulerTaskWarehouse("TEST_WH");
    assertInternalStatus(PAUSED, FINALIZED);

    // cleanup
    dropScheduler();
  }

  private void configureConnector() {
    var config =
        "{\"warehouse\": \""
            + WAREHOUSE
            + "\", \"global_schedule\": {\"scheduleType\":"
            + "\"CRON\", \"scheduleDefinition\": \"*/10 * * * *\"}}";
    callProcedure(format("CONFIGURE_CONNECTOR(PARSE_JSON('%s'))", config));
    assertConnectorConfigSavedWarehouse(WAREHOUSE);
    setConnectorStatus(STARTED, FINALIZED);
  }

  private void pauseConnector() {
    setConnectorStatus(PAUSED, FINALIZED);
    TaskRef.of(session, SCHEDULER_TASK).suspendIfExists();
  }

  private void createScheduler() {
    var response = callProcedure("CREATE_SCHEDULER()");
    NativeSdkAssertions.assertThatResponseMap(response).hasOKResponseCode();
    assertSchedulerTaskWarehouse("reference('WAREHOUSE_REFERENCE')");

    // Grant ALL on the scheduler task, so it can be managed and validated easily
    executeInApp(
        format("GRANT ALL ON TASK %s TO APPLICATION ROLE ADMIN", SCHEDULER_TASK.getValue()));
  }

  // Only the task owner can drop it, we cannot use TaskRef here
  private void dropScheduler() {
    executeInApp(format("DROP TASK IF EXISTS %s", SCHEDULER_TASK.getValue()));
  }

  private void assertSchedulerTaskWarehouse(String warehouse) {
    var task = TaskLister.getInstance(session).showTask(SCHEDULER_TASK);
    assertThat(task).isNotEmpty().get(TASK_PROPERTIES).hasWarehouse(warehouse);
  }

  private void assertConnectorConfigSavedWarehouse(String warehouse) {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).inPath("warehouse").isEqualTo(warehouse);
  }
}
