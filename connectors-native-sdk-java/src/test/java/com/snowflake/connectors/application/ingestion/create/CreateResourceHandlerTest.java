/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.create;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.SNAPSHOT;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.INGESTION_PROCESS;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.snowflake.connectors.application.ingestion.definition.InMemoryResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.ResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CreateResourceHandlerTest {

  private static final Variant RESOURCE_ID =
      new Variant(Map.of("key1", "value1", "key2", "value2"));
  private static final Variant RESOURCE_METADATA = new Variant(Map.of("aa", "dd", "bb", "eee"));
  private static final Variant INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  entry("id", "ingestionConfig"),
                  entry("ingestionStrategy", "SNAPSHOT"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "10m"),
                  entry("customIngestionConfiguration", Map.of("prop", "value")),
                  entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  entry("id", "secondConfig"),
                  entry("ingestionStrategy", "INCREMENTAL"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "20m"))));

  private static final List<IngestionConfiguration<Variant, Variant>>
      INGESTION_CONFIGURATION_OBJECT =
          List.of(
              new IngestionConfiguration<>(
                  "ingestionConfig",
                  SNAPSHOT,
                  new Variant(Map.of("prop", "value")),
                  INTERVAL,
                  "10m",
                  new Variant(Map.of("table", "name"))),
              new IngestionConfiguration<>(
                  "secondConfig", INCREMENTAL, null, INTERVAL, "20m", null));

  private static final VariantResource VARIANT_RESOURCE =
      new VariantResource(
          "id",
          "name",
          true,
          RESOURCE_ID,
          new Variant(Map.of("aa", "dd", "bb", "eee")),
          List.of(
              new IngestionConfiguration<>(
                  "idd",
                  INCREMENTAL,
                  new Variant(Map.of("aa", "dd")),
                  INTERVAL,
                  "10m",
                  new Variant("example"))));
  private static final String NAME = "some name";

  private ResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository;
  private InMemoryIngestionProcessRepository ingestionProcessRepository;
  private CreateResourceHandlerBuilder handlerBuilder;

  @BeforeEach
  void setUp() {
    resourceIngestionDefinitionRepository = new InMemoryResourceIngestionDefinitionRepository<>();
    ingestionProcessRepository = new InMemoryIngestionProcessRepository();
    handlerBuilder =
        new CreateResourceHandlerTestBuilder()
            .withErrorHelper(ConnectorErrorHelper.builder(null, "RESOURCE").build())
            .withResourceIngestionDefinitionRepository(resourceIngestionDefinitionRepository)
            .withIngestionProcessRepository(ingestionProcessRepository)
            .withPreCreateResourceCallback(x -> ConnectorResponse.success())
            .withPostCreateResourceCallback(x -> ConnectorResponse.success())
            .withCreateResourceValidator(x -> ConnectorResponse.success())
            .withTransactionManager(new InMemoryTransactionManager());
  }

  @Test
  void shouldCreateAResource() {
    // given
    String id = "theBestId";
    Variant resourceMetadata = new Variant(Map.of("meta", "data"));
    var handler = handlerBuilder.build();

    // when
    var response =
        handler.createResource(
            id, NAME, true, RESOURCE_ID, resourceMetadata, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasOKResponseCode()
        .hasMessage("Resource created")
        .hasAdditionalPayload("id", id);
    assertResourceIngestionDefinitionIsCreated(id);
    assertProcessesAreCreated(id, INGESTION_CONFIGURATION_OBJECT);
  }

  @Test
  void shouldCreateAResourceWithDefaultValues() {
    // given
    var handler = handlerBuilder.build();

    // when
    var response =
        handler.createResource(
            null,
            NAME,
            false,
            new Variant(RESOURCE_ID.asJsonString()),
            null,
            new Variant(INGESTION_CONFIGURATIONS.asJsonString()));

    // then
    assertThat(response).hasOKResponseCode().hasMessage("Resource created");
    String id = extractIdFromResponse(response);
    assertResourceIngestionDefinitionIsCreated(id);
  }

  @Test
  void shouldReturnErrorWhenIngestionConfigurationHasWrongStructureAndCannotBeDeserialized() {
    // given
    String invalidConfiguration = "invalid";
    var handler = handlerBuilder.build();

    // when
    ConnectorResponse response =
        handler.createResource(
            null, "name", true, RESOURCE_ID, null, new Variant(invalidConfiguration));

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage(
            "Provided ingestion configuration has invalid structure and cannot be processed.");
  }

  @Test
  void shouldReturnErrorWhenAResourceWithGivenIdAlreadyExists() {
    // given
    String id = "id";
    resourceIngestionDefinitionRepository.save(VARIANT_RESOURCE);
    var handler = handlerBuilder.build();

    // when
    ConnectorResponse response =
        handler.createResource(id, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with id '" + id + "' already exists.");
  }

  @Test
  void shouldReturnErrorWhenResourceWithGivenResourceIdAlreadyExists() {
    // given
    String id = "id2";
    resourceIngestionDefinitionRepository.save(VARIANT_RESOURCE);
    var handler = handlerBuilder.build();

    // when
    ConnectorResponse response =
        handler.createResource(id, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with resourceId '" + RESOURCE_ID + "' already exists.");
  }

  @Test
  void shouldNotCreateAnyProcessForDisabledResource() {
    // given
    var id = "theBestId";
    boolean enabled = false;
    var handler = handlerBuilder.build();

    // when
    ConnectorResponse response =
        handler.createResource(
            id, "some name", enabled, RESOURCE_ID, RESOURCE_METADATA, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasOKResponseCode().hasMessage("Resource created");

    assertThat(resourceIngestionDefinitionRepository.fetch(id).orElse(null))
        .hasId(id)
        .hasName(NAME)
        .isEnabled(false)
        .hasResourceId(RESOURCE_ID)
        .hasResourceMetadata(RESOURCE_METADATA)
        .hasIngestionConfigurations(INGESTION_CONFIGURATION_OBJECT);
    assertNoResourceIsCreated();
  }

  @Test
  void shouldNotCreateResourceWhenValidatorFails() {
    // given
    String id = randomId();
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder
            .withCreateResourceValidator(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response =
        handler.createResource(null, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionHasNotBeenCreated(id);
  }

  @Test
  void shouldNotCreateResourceWhenPreCallbackFails() {
    // given
    String id = randomId();
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder
            .withPreCreateResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response =
        handler.createResource(null, "name", true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionHasNotBeenCreated(id);
  }

  @Test
  void shouldReturnErrorWhenPostCallbackFails() {
    // given
    String id = randomId();
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder
            .withPostCreateResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response =
        handler.createResource(id, NAME, true, RESOURCE_ID, null, INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode(errorCode)
        .hasMessage(errorMessage)
        .hasAdditionalPayload("id", id);
    assertResourceIngestionDefinitionIsCreated(id);
    assertProcessesAreCreated(id, INGESTION_CONFIGURATION_OBJECT);
  }

  private void assertResourceIngestionDefinitionHasNotBeenCreated(String id) {
    Optional<VariantResource> result = resourceIngestionDefinitionRepository.fetch(id);
    assertThat(result).isEmpty();
  }

  private String extractIdFromResponse(ConnectorResponse response) {
    var id = response.getAdditionalPayload().get("id");
    assertNotNull(id);
    return id.toString();
  }

  private void assertResourceIngestionDefinitionIsCreated(String id) {
    var count =
        resourceIngestionDefinitionRepository.fetchAll().stream()
            .filter(
                resource ->
                    Objects.equals(resource.getId(), id)
                        && Objects.equals(resource.getResourceId(), RESOURCE_ID)
                        && Objects.equals(resource.getName(), NAME))
            .count();
    assertThat(count).isEqualTo(1);
  }

  private void assertNoResourceIsCreated() {
    var count = ingestionProcessRepository.getRepository().size();
    assertThat(count).isEqualTo(0);
  }

  private void assertProcessesAreCreated(
      String resourceIngestionDefinitionId,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    ingestionConfigurations.forEach(
        ingestionConfiguration -> {
          List<IngestionProcess> processes =
              ingestionProcessRepository.fetchAll(
                  resourceIngestionDefinitionId, ingestionConfiguration.getId(), "DEFAULT");
          assertThat(processes).hasSize(1).singleElement(INGESTION_PROCESS).hasStatus("SCHEDULED");
        });
  }
}
