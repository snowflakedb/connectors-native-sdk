/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.dailyuse;

import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import java.util.Arrays;
import java.util.List;
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
    assertThat(procedureResponse).hasOkResponseCode();
    assertTaskWasCreated();
  }

  private void assertTaskWasCreated() {
    var tasks =
        session
            .sql("SHOW TASKS IN APPLICATION " + application.instanceName)
            .select("\"name\"")
            .collect();
    List<String> taskNames = Arrays.stream(tasks).map(task -> task.getString(0)).collect(toList());
    Assertions.assertThat(taskNames).contains("SCHEDULER_TASK");
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
