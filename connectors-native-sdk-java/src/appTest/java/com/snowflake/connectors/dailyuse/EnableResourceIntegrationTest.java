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

public class EnableResourceIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final String INGESTION_CONFIGURATION_ID_1 = randomId();
  private static final String INGESTION_CONFIGURATION_ID_2 = randomId();

  @Test
  void shouldEnableAResource() {
    // given
    String definitionId = disabledResourceIngestionDefinitionExists();

    // when
    String query = String.format("ENABLE_RESOURCE('%s')", definitionId);
    var response = callProcedure(query);

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertThatNewIngestionProcessWasCreated(definitionId, INGESTION_CONFIGURATION_ID_1);
    assertThatNewIngestionProcessWasCreated(definitionId, INGESTION_CONFIGURATION_ID_2);
  }

  private String disabledResourceIngestionDefinitionExists() {
    String id = randomId();
    Variant ingestionConfigurations =
        new Variant(
            List.of(
                Map.ofEntries(
                    entry("id", INGESTION_CONFIGURATION_ID_1),
                    entry("ingestionStrategy", "SNAPSHOT"),
                    entry("scheduleType", "INTERVAL"),
                    entry("scheduleDefinition", "10m")),
                Map.ofEntries(
                    entry("id", INGESTION_CONFIGURATION_ID_2),
                    entry("ingestionStrategy", "INCREMENTAL"),
                    entry("scheduleType", "INTERVAL"),
                    entry("scheduleDefinition", "20m"))));
    var resourceId = new Variant(Map.of("property", "value"));
    String query =
        String.format(
            "CREATE_RESOURCE('name', PARSE_JSON('%s'), PARSE_JSON('%s'), '%s', false)",
            resourceId.asJsonString(), ingestionConfigurations.asJsonString(), id);
    var response = callProcedure(query);
    assertThatResponseMap(response).hasOKResponseCode();
    return id;
  }

  private void assertResourceIngestionDefinitionIsEnabled(String definitionId) {
    String query =
        String.format(
            "select count(*) from public.ingestion_definitions where id = '%s' and is_enabled ="
                + " true",
            definitionId);
    var count = session.sql(query).collect()[0].getInt(0);
    Assertions.assertThat(count).isEqualTo(1);
  }

  private void assertThatNewIngestionProcessWasCreated(
      String definitionId, String configurationId) {
    String query =
        String.format(
            "select count(*) from state.ingestion_process where resource_ingestion_definition_id ="
                + " '%s' and ingestion_configuration_id = '%s' and status = 'SCHEDULED'",
            definitionId, configurationId);
    var count = executeInApp(query)[0].getInt(0);
    Assertions.assertThat(count).isEqualTo(1);
  }
}
