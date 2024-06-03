/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.dailyuse;

import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class CreateResourceIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final String NAME = "some name";
  private static final Variant RESOURCE_ID = new Variant(Map.of("property", "value"));
  private static final Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  entry("id", "ingestionConfig"),
                  entry("ingestionStrategy", "SNAPSHOT"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "10m"),
                  entry("customIngestionConfiguration", Map.of("property", "value")),
                  entry("destination", Map.of("tableName", "name"))),
              Map.ofEntries(
                  entry("id", "secondConfig"),
                  entry("ingestionStrategy", "INCREMENTAL"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "20m"))));

  @Test
  void shouldCreateAResource() {
    // given
    String id = "theBestId";
    Variant resourceMetadata = new Variant(Map.of("meta", "data"));

    // when
    String query =
        String.format(
            "CREATE_RESOURCE('%s', PARSE_JSON('%s'), PARSE_JSON('%s'), '%s', true,"
                + " PARSE_JSON('%s'))",
            NAME,
            RESOURCE_ID.asJsonString(),
            INGESTION_CONFIGURATIONS.asJsonString(),
            id,
            resourceMetadata.asJsonString());
    var response = callProcedure(query);

    // then
    assertThat(response).hasOkResponseCode().hasMessage("Resource created").hasId(id);
    assertResourceIsReturnedFromView(id);
  }

  @Test
  void shouldCreateAResourceWithDefaultValues() {
    // when
    String query =
        String.format(
            "CREATE_RESOURCE('%s', PARSE_JSON('%s'), PARSE_JSON('%s'))",
            NAME, RESOURCE_ID.asJsonString(), INGESTION_CONFIGURATIONS.asJsonString());
    var response = callProcedure(query);

    // then
    assertThat(response).hasOkResponseCode().hasMessage("Resource created");
    String id = extractIdFromResponse(response);
    assertResourceIsReturnedFromView(id);
  }

  private String extractIdFromResponse(Map<String, Variant> response) {
    var id = response.get("id");
    assertNotNull(id);
    return id.toString();
  }

  private void assertResourceIsReturnedFromView(String id) {
    String query =
        String.format(
            "select count(*) from public.ingestion_definitions where id = '%s' and resource_id ="
                + " '%s' and name = '%s'",
            id, RESOURCE_ID.asJsonString(), NAME);
    var count = session.sql(query).collect()[0].getInt(0);
    Assertions.assertThat(count).isEqualTo(1);
  }
}
