/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.SNAPSHOT;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.sql.TimestampUtil.toInstant;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;
import static java.lang.String.format;
import static java.util.Map.entry;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class UpdateResourceIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final Variant RESOURCE_ID =
      new Variant(Map.of("key1", "value1", "key2", "value2"));
  private static final String RESOURCE_INGESTION_DEFINITION_ID = "id";
  private static final Variant RESOURCE_METADATA = new Variant(Map.of("aa", "dd", "bb", "eee"));
  private static final Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  entry("id", "1"),
                  entry("ingestionStrategy", "SNAPSHOT"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "10m"),
                  entry("customIngestionConfiguration", Map.of("prop", "value")),
                  entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  entry("id", "2"),
                  entry("ingestionStrategy", "INCREMENTAL"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "20m"))));
  private static final Variant UPDATED_INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  entry("id", "2"),
                  entry("ingestionStrategy", "SNAPSHOT"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "15m"),
                  entry("customIngestionConfiguration", Map.of("key", "value")),
                  entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  entry("id", "3"),
                  entry("ingestionStrategy", "INCREMENTAL"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "25m"))));

  @Test
  void shouldUpdateResourceAndManageItsIngestionProcesses() {
    // given
    createResource(
        RESOURCE_ID,
        INGESTION_CONFIGURATIONS,
        RESOURCE_INGESTION_DEFINITION_ID,
        true,
        RESOURCE_METADATA);

    // when
    var result = updateResource(RESOURCE_INGESTION_DEFINITION_ID, UPDATED_INGESTION_CONFIGURATIONS);

    // then
    assertThatResponseMap(result).hasOKResponseCode().hasMessage("Resource successfully updated.");

    // and
    assertResourceIngestionConfigurations(
        RESOURCE_INGESTION_DEFINITION_ID,
        List.of(
            new IngestionConfiguration<>(
                "2",
                SNAPSHOT,
                new Variant(Map.of("key", "value")),
                INTERVAL,
                "15m",
                new Variant(Map.of("table", "name"))),
            new IngestionConfiguration<>("3", INCREMENTAL, null, INTERVAL, "25m", null)));

    // and
    var ingestionProcesses = getIngestionProcesses(RESOURCE_INGESTION_DEFINITION_ID);
    assertIngestionProcessExists("1", FINISHED, ingestionProcesses);
    assertIngestionProcessExists("2", SCHEDULED, ingestionProcesses);
    assertIngestionProcessExists("3", SCHEDULED, ingestionProcesses);
  }

  private Map<String, Variant> updateResource(
      String resourceIngestionDefinitionId, Variant updatedIngestionConfigurations) {
    var query =
        format(
            "UPDATE_RESOURCE('%s', PARSE_JSON('%s'))",
            resourceIngestionDefinitionId, updatedIngestionConfigurations.asJsonString());
    var result = callProcedure(query);
    assertThatResponseMap(result).hasOKResponseCode();
    return result;
  }

  private void createResource(
      Variant resourceId,
      Variant ingestionConfigurations,
      String resourceIngestionDefinitionId,
      boolean enabled,
      Variant metadata) {
    var query =
        format(
            "CREATE_RESOURCE('%s', PARSE_JSON('%s'), PARSE_JSON('%s'), '%s', '%s',"
                + " PARSE_JSON('%s'))",
            "name",
            resourceId.asJsonString(),
            ingestionConfigurations.asJsonString(),
            resourceIngestionDefinitionId,
            enabled,
            metadata.asJsonString());
    var result = callProcedure(query);
    assertThatResponseMap(result).hasOKResponseCode();
  }

  private VariantResource getResource(String resourceIngestionDefinitionId) {
    var query =
        String.format(
            "SELECT * FROM state.resource_ingestion_definition WHERE id=%s;",
            resourceIngestionDefinitionId);
    var response = executeInApp(query);
    assertThat(response.length).isEqualTo(1);
    return mapVariantResource(response[0]);
  }

  private VariantResource mapVariantResource(Row row) {
    return new VariantResource(
        row.getString(0),
        row.getString(1),
        row.getBoolean(2),
        row.getVariant(4),
        row.getVariant(5),
        mapIngestionConfigurations(row.getVariant(6)));
  }

  private List<IngestionConfiguration<Variant, Variant>> mapIngestionConfigurations(
      Variant ingestionConfigurations) {
    var typeFactory = TypeFactory.defaultInstance();
    var javaType =
        typeFactory.constructParametricType(
            IngestionConfiguration.class, Variant.class, Variant.class);
    var ingestionConfigurationListType = typeFactory.constructParametricType(List.class, javaType);
    return mapVariant(ingestionConfigurations, ingestionConfigurationListType);
  }

  private void assertResourceIngestionConfigurations(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> expectedIngestionConfigurations) {
    var actualResourceIngestionDefinition = getResource(resourceIngestionDefinitionId);
    assertThat(actualResourceIngestionDefinition)
        .hasIngestionConfigurations(expectedIngestionConfigurations);
  }

  private void assertIngestionProcessExists(
      String ingestionConfigurationId, String status, List<IngestionProcess> ingestionProcesses) {
    var matchingIngestionProcessesNumber =
        ingestionProcesses.stream()
            .filter(
                process ->
                    process.getIngestionConfigurationId().equals(ingestionConfigurationId)
                        && process.getStatus().equals(status))
            .count();
    assertThat(matchingIngestionProcessesNumber).isEqualTo(1);
  }

  private List<IngestionProcess> getIngestionProcesses(String resourceIngestionDefinitionId) {
    var query =
        format(
            "SELECT * FROM state.ingestion_process WHERE"
                + " resource_ingestion_definition_id='%s';",
            resourceIngestionDefinitionId);
    var response = executeInApp(query);
    List<IngestionProcess> ingestionProcesses =
        Arrays.stream(response).map(this::mapToIngestionProcess).collect(Collectors.toList());
    assertThat(response.length).isGreaterThan(0);
    return ingestionProcesses;
  }

  private IngestionProcess mapToIngestionProcess(Row row) {
    return new IngestionProcess(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        toInstant(row.getTimestamp(5)),
        toInstant(row.getTimestamp(7)),
        Optional.ofNullable(row.get(8)).map(it -> row.getVariant(7)).orElse(null));
  }
}
