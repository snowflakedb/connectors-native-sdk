/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.dailyuse;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static java.util.Map.entry;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class DisableResourceIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldDisableAResource() {
    // given
    String resourceIngestionDefinitionId = resourceIngestionDefinitionExists();

    // when
    String query = String.format("DISABLE_RESOURCE('%s')", resourceIngestionDefinitionId);
    var response = callProcedure(query);

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsDisabled(resourceIngestionDefinitionId);
    assertIngestionProcessesAreFinished(resourceIngestionDefinitionId);
  }

  private String resourceIngestionDefinitionExists() {
    String id = randomId();
    Variant ingestionConfigurations =
        new Variant(
            List.of(
                Map.ofEntries(
                    entry("id", "ingestionConfig"),
                    entry("ingestionStrategy", "SNAPSHOT"),
                    entry("scheduleType", "INTERVAL"),
                    entry("scheduleDefinition", "10m")),
                Map.ofEntries(
                    entry("id", "secondConfig"),
                    entry("ingestionStrategy", "INCREMENTAL"),
                    entry("scheduleType", "INTERVAL"),
                    entry("scheduleDefinition", "20m"))));
    var resourceId = new Variant(Map.of("property", "value"));
    String query =
        String.format(
            "CREATE_RESOURCE('name', PARSE_JSON('%s'), PARSE_JSON('%s'), '%s', true)",
            resourceId.asJsonString(), ingestionConfigurations.asJsonString(), id);
    var response = callProcedure(query);
    assertThatResponseMap(response).hasOKResponseCode();
    return id;
  }

  private void assertResourceIngestionDefinitionIsDisabled(String resourceIngestionDefinitionId) {
    String query =
        String.format(
            "select count(*) from public.ingestion_definitions where id = '%s' and is_enabled ="
                + " false",
            resourceIngestionDefinitionId);
    var count = session.sql(query).collect()[0].getInt(0);
    Assertions.assertThat(count).isEqualTo(1);
  }

  private void assertIngestionProcessesAreFinished(String resourceIngestionDefinitionId) {
    String query =
        String.format(
            "select count(*) from state.ingestion_process where resource_ingestion_definition_id ="
                + " '%s' and status = 'FINISHED'",
            resourceIngestionDefinitionId);
    var count = executeInApp(query)[0].getInt(0);
    Assertions.assertThat(count).isEqualTo(2);
  }
}
