/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessRepository;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class CreateResourceHandlerTest {
  private static VariantResource VARIANT_RESOURCE =
      new VariantResource(
          "id",
          "name",
          true,
          new Variant("resource_id"),
          new Variant(Map.of("aa", "dd", "bb", "eee")),
          List.of(
              new IngestionConfiguration<>(
                  "idd",
                  INCREMENTAL,
                  new Variant(Map.of("aa", "dd")),
                  INTERVAL,
                  "10m",
                  new Variant("example"))));
  private static Variant RESOURCE_ID = new Variant(Map.of("key1", "value1", "key2", "value2"));
  private static Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  Map.entry("id", "idd"),
                  Map.entry("ingestionStrategy", "INCREMENTAL"),
                  Map.entry("scheduleType", "INTERVAL"),
                  Map.entry("scheduleDefinition", "10m"))));

  @SuppressWarnings("unchecked")
  ResourceIngestionDefinitionRepository<VariantResource> resourceIngestionDefinitionRepository =
      mock(ResourceIngestionDefinitionRepository.class);

  IngestionProcessRepository ingestionProcessRepository = mock(IngestionProcessRepository.class);

  CreateResourceHandler resourceHandler =
      new CreateResourceHandlerTestBuilder()
          .withResourceIngestionDefinitionRepository(resourceIngestionDefinitionRepository)
          .withIngestionProcessRepository(ingestionProcessRepository)
          .withErrorHelper(ConnectorErrorHelper.builder(null, "RESOURCE").build())
          .build();

  @Test
  public void
      shouldReturnErrorWhenIngestionConfigurationHasWrongStructureAndCannotBeDeserialized() {
    // given
    String invalidConfiguration = "invalid";

    when(resourceIngestionDefinitionRepository.fetch(anyString())).thenReturn(Optional.empty());
    when(resourceIngestionDefinitionRepository.fetchByResourceId(any()))
        .thenReturn(Optional.empty());

    // when
    ConnectorResponse response =
        resourceHandler.createResource(
            null, "name", true, RESOURCE_ID, null, new Variant(invalidConfiguration));

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage(
            "Provided ingestion configuration has invalid structure and cannot be processed.");
  }

  @Test
  public void shouldReturnErrorWhenAResourceWithGivenIdAlreadyExists() {
    // given
    String id = "id";
    when(resourceIngestionDefinitionRepository.fetch(id)).thenReturn(Optional.of(VARIANT_RESOURCE));

    // when
    ConnectorResponse response =
        resourceHandler.createResource(
            id, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with id '" + id + "' already exists.");
  }

  @Test
  public void shouldReturnErrorWhenResourceWithGivenResourceIdAlreadyExists() {
    // given
    String id = "id";
    when(resourceIngestionDefinitionRepository.fetch(id)).thenReturn(Optional.empty());
    when(resourceIngestionDefinitionRepository.fetchByResourceId(RESOURCE_ID))
        .thenReturn(Optional.of(VARIANT_RESOURCE));

    // when
    ConnectorResponse response =
        resourceHandler.createResource(
            id, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with resourceId '" + RESOURCE_ID + "' already exists.");
  }
}
