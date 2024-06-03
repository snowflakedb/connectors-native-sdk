/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UpdateWarehouseTest extends BaseNativeSdkIntegrationTest {

  @BeforeAll
  @Override
  public void beforeAll() throws IOException {
    super.beforeAll();

    session
        .sql(
            "CREATE WAREHOUSE IF NOT EXISTS TEST_WH "
                + "WAREHOUSE_SIZE=XSMALL "
                + "AUTO_SUSPEND=1800 "
                + "SERVER_TYPE='C6GD2XLARGE'")
        .collect();
    application.grantUsageOnWarehouse("TEST_WH");
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
    assertSchedulerTaskWarehouse("\"TEST_WH\"");
    assertInternalStatus(PAUSED, FINALIZED);

    // cleanup
    dropScheduler();
  }

  @Test
  void shouldReturnErrorIfConnectorIsNotPaused() {
    // given
    configureConnector();

    // expect
    Assertions.assertThatThrownBy(() -> callProcedure("UPDATE_WAREHOUSE('TEST_WH')"))
        .hasMessageContaining(
            "Invalid connector status. Expected status: [PAUSED]. Current status:" + " STARTED.");
  }

  @ParameterizedTest
  @ValueSource(strings = {"''", "NULL"})
  void shouldReturnErrorIfWarehouseNameEmptyOrNull(String warehouse) {
    // given
    pauseConnector();

    // expect
    Assertions.assertThatThrownBy(() -> callProcedure(format("UPDATE_WAREHOUSE(%s)", warehouse)))
        .hasMessageContaining("Provided identifier must not be empty");
  }

  @Test
  void shouldReturnErrorIfWarehouseAlreadyUsed() {
    // given
    configureConnector();
    pauseConnector();

    // expect
    Assertions.assertThatThrownBy(() -> callProcedure("UPDATE_WAREHOUSE('XS')"))
        .hasMessageContaining("Warehouse \"XS\" is already used by the application");
  }

  @Test
  void shouldReturnErrorIfApplicationHasNoAccessToWarehouse() {
    // given
    configureConnector();
    pauseConnector();
    application.revokeUsageOnWarehouse("TEST_WH");

    // expect
    Assertions.assertThatThrownBy(() -> callProcedure("UPDATE_WAREHOUSE('TEST_WH')"))
        .hasMessageContaining("Warehouse \"TEST_WH\" is inaccessible to the application");

    // cleanup
    application.grantUsageOnWarehouse("TEST_WH");
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
        format("GRANT ALL ON TASK %s TO APPLICATION ROLE ADMIN", SCHEDULER_TASK.getEscapedName()));
  }

  // Only the task owner can drop it, we cannot use TaskRef here
  private void dropScheduler() {
    executeInApp(format("DROP TASK IF EXISTS %s", SCHEDULER_TASK.getEscapedName()));
  }

  private void assertSchedulerTaskWarehouse(String warehouse) {
    var task = TaskLister.getInstance(session).showTask(SCHEDULER_TASK);
    assertThat(task).isNotEmpty();
    assertThat(task.get()).hasWarehouse(warehouse);
  }

  private void assertConnectorConfigSavedWarehouse(String warehouse) {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).inPath("warehouse").isEqualTo(warehouse);
  }
}
