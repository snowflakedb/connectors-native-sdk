/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;
import static java.lang.String.format;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateResourceHandlerIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final String ID = "id";
  private static final Variant RESOURCE_ID = new Variant(Map.of("tableName", "aaa"));
  private static final Variant RESOURCE_METADATA = new Variant(Map.of("aa", "dd", "bb", "eee"));
  private static final String NAME = "name";
  private static final List<IngestionConfiguration<Variant, Variant>>
      INGESTION_CONFIGURATION_OBJECT =
          List.of(
              new IngestionConfiguration<>(
                  "firstConfig",
                  INCREMENTAL,
                  new Variant(Map.of("prop", "value")),
                  INTERVAL,
                  "10m",
                  new Variant(Map.of("table", "name"))),
              new IngestionConfiguration<>(
                  "secondConfig", INCREMENTAL, null, INTERVAL, "20m", null));

  private static final Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  Map.entry("id", "firstConfig"),
                  Map.entry("ingestionStrategy", "INCREMENTAL"),
                  Map.entry("scheduleType", "INTERVAL"),
                  Map.entry("scheduleDefinition", "10m"),
                  Map.entry("customIngestionConfiguration", Map.of("prop", "value")),
                  Map.entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  Map.entry("id", "secondConfig"),
                  Map.entry("ingestionStrategy", "INCREMENTAL"),
                  Map.entry("scheduleType", "INTERVAL"),
                  Map.entry("scheduleDefinition", "20m"))));

  @Test
  public void
      shouldCreateResourceIngestionDefinitionAndIngestionProcess_ForEachIngestionConfigurationWhenResourceIsEnabled() {
    // when
    Variant result = new Variant(createResource());
    Row actualResourceIngestionDefinition = getActualResourceIngestionDefinition();

    // then
    assertThatResponseMap(result.asMap())
        .hasOKResponseCode()
        .hasMessage("Resource created")
        .hasId(ID);
    assertThat(mapVariantResource(actualResourceIngestionDefinition))
        .hasId(ID)
        .hasName(NAME)
        .isEnabled(true)
        .hasResourceId(RESOURCE_ID)
        .hasResourceMetadata(RESOURCE_METADATA)
        .hasIngestionConfigurations(INGESTION_CONFIGURATION_OBJECT);
    assertIngestionProcessIsCreatedForEachConfiguration();
  }

  Row getActualResourceIngestionDefinition() {
    var query = String.format("SELECT * FROM state.resource_ingestion_definition WHERE id=%s;", ID);
    var response = executeInApp(query);
    Assertions.assertThat(response.length).isEqualTo(1);
    return response[0];
  }

  void assertIngestionProcessIsCreatedForEachConfiguration() {
    assertIngestionProcessIsCreated("firstConfig");
    assertIngestionProcessIsCreated("secondConfig");
  }

  void assertIngestionProcessIsCreated(String ingestionConfigurationId) {
    var query =
        format(
            "SELECT status, type FROM state.ingestion_process WHERE"
                + " resource_ingestion_definition_id='%s' AND ingestion_configuration_id='%s' AND"
                + " type='%s';",
            ID, ingestionConfigurationId, "DEFAULT");
    var response = executeInApp(query);
    Assertions.assertThat(response.length).isEqualTo(1);
    var process = response[0];
    assertThat(process.getString(0)).isEqualTo("SCHEDULED");
    assertThat(process.getString(1)).isEqualTo("DEFAULT");
  }

  private Map<String, Variant> createResource() {
    var query =
        format(
            "CREATE_RESOURCE('%s', PARSE_JSON('%s'), PARSE_JSON('%s'), '%s', '%s',"
                + " PARSE_JSON('%s'))",
            NAME,
            RESOURCE_ID.asJsonString(),
            INGESTION_CONFIGURATIONS.asJsonString(),
            ID,
            true,
            RESOURCE_METADATA.asJsonString());
    return callProcedure(query);
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
}
