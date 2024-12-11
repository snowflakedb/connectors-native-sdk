/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class CreateSchedulerIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldCreateScheduler() {
    // given
    setConnectorConfiguration();
    setupWarehouseReference();

    // when
    var procedureResponse = callProcedure("CREATE_SCHEDULER()");

    // then
    assertThatResponseMap(procedureResponse).hasOKResponseCode();
    assertTaskWasCreated();
  }

  private void assertTaskWasCreated() {
    var tasks =
        executeInApp(
            format(
                "SHOW TASKS LIKE '%s' IN SCHEMA %s",
                SCHEDULER_TASK.getName().getUnquotedValue(),
                SCHEDULER_TASK.getSchema().get().getValue()));
    Assertions.assertThat(tasks).hasSize(1);
  }

  private void setConnectorConfiguration() {
    String configJson =
        "{\"global_schedule\": {\"scheduleType\": \"CRON\", \"scheduleDefinition\": \"*/10 * * *"
            + " *\"}}";

    executeInApp(
        "INSERT INTO STATE.APP_CONFIG (KEY, VALUE, UPDATED_AT) "
            + " SELECT 'connector_configuration', PARSE_JSON('"
            + configJson
            + "'), SYSDATE()");
  }
}
