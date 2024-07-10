/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.update;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.SNAPSHOT;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.util.Map.entry;

import com.snowflake.connectors.application.ingestion.definition.InMemoryResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfigurationMapper;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class UpdateResourceHandlerTest {

  private static final Variant RESOURCE_ID =
      new Variant(Map.of("key1", "value1", "key2", "value2"));
  private static final String RESOURCE_INGESTION_DEFINITION_ID = "id";
  private static final Variant RESOURCE_METADATA = new Variant(Map.of("aa", "dd", "bb", "eee"));
  private static final List<IngestionConfiguration<Variant, Variant>>
      MAPPED_INGESTION_CONFIGURATIONS =
          List.of(
              new IngestionConfiguration<>(
                  "1",
                  SNAPSHOT,
                  new Variant(Map.of("prop", "value")),
                  INTERVAL,
                  "10m",
                  new Variant(Map.of("table", "name"))),
              new IngestionConfiguration<>("2", INCREMENTAL, null, INTERVAL, "20m", null));
  private static final Variant RAW_INGESTION_CONFIGURATIONS =
      new Variant(
          List.of(
              Map.ofEntries(
                  entry("id", "1"),
                  entry("ingestionStrategy", "SNAPSHOT"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "10m"),
                  entry("customIngestionConfiguration", Map.of("table", "name")),
                  entry("destination", Map.of("table", "name"))),
              Map.ofEntries(
                  entry("id", "2"),
                  entry("ingestionStrategy", "INCREMENTAL"),
                  entry("scheduleType", "INTERVAL"),
                  entry("scheduleDefinition", "20m"))));
  private static final Variant RAW_UPDATED_INGESTION_CONFIGURATIONS =
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
  private static final VariantResource VARIANT_RESOURCE =
      new VariantResource(
          RESOURCE_INGESTION_DEFINITION_ID,
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
  private static final String ERROR_CODE = "ERROR";
  private static final String ERROR_MSG = "error message";
  private final InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  private final InMemoryResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository = new InMemoryResourceIngestionDefinitionRepository<>();

  @AfterEach
  void cleanUp() {
    resourceIngestionDefinitionRepository.clear();
    ingestionProcessRepository.clear();
  }

  @Test
  void shouldUpdateResourceAndManageIngestionProcessesIfResourceIsEnabled() {
    // given
    var updateResourceHandler = handlerBuilder().build();
    createResource(
        RESOURCE_INGESTION_DEFINITION_ID,
        "name",
        true,
        RESOURCE_ID,
        RESOURCE_METADATA,
        MAPPED_INGESTION_CONFIGURATIONS);

    // when
    ConnectorResponse response =
        updateResourceHandler.updateResource(
            RESOURCE_INGESTION_DEFINITION_ID, RAW_UPDATED_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode("OK").hasMessage("Resource successfully updated.");
    assertIngestionProcessExists(RESOURCE_INGESTION_DEFINITION_ID, "1", FINISHED);
    assertIngestionProcessExists(RESOURCE_INGESTION_DEFINITION_ID, "2", SCHEDULED);
    assertIngestionProcessExists(RESOURCE_INGESTION_DEFINITION_ID, "3", SCHEDULED);
    assertIngestionConfigurationsAreEqual(
        RESOURCE_INGESTION_DEFINITION_ID, RAW_UPDATED_INGESTION_CONFIGURATIONS);
  }

  @Test
  void shouldUpdateResourceWithoutManagingIngestionProcessesIfResourceIsDisabled() {
    // given
    var updateResourceHandler = handlerBuilder().build();
    createResource(
        RESOURCE_INGESTION_DEFINITION_ID,
        "name",
        true,
        RESOURCE_ID,
        RESOURCE_METADATA,
        MAPPED_INGESTION_CONFIGURATIONS);
    var processesBeforeUpdate =
        ingestionProcessRepository.fetchAll(RESOURCE_INGESTION_DEFINITION_ID);

    // when
    ConnectorResponse response =
        updateResourceHandler.updateResource(
            RESOURCE_INGESTION_DEFINITION_ID, RAW_UPDATED_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode("OK").hasMessage("Resource successfully updated.");
    assertCurrentIngestionProcessesAreExactly(
        RESOURCE_INGESTION_DEFINITION_ID, processesBeforeUpdate);
    assertIngestionConfigurationsAreEqual(
        RESOURCE_INGESTION_DEFINITION_ID, RAW_UPDATED_INGESTION_CONFIGURATIONS);
  }

  @Test
  void shouldReturnErrorWhenProvidedIngestionConfigurationsHaveInvalidStructure() {
    // given
    String id = randomId();
    Variant invalidConfig = new Variant("invalid");
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.updateResource(id, invalidConfig);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage(
            "Provided ingestion configuration has invalid structure and cannot be processed.");
  }

  @Test
  void shouldReturnErrorWhenResourceDoesNotExist() {
    // given
    String id = randomId();
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.updateResource(id, RAW_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with resourceId '" + id + "' does not exist");
  }

  @Test
  void shouldNotUpdateResourceWhenValidatorReturnsError() {
    // given
    String id = randomId();
    var handler =
        handlerBuilder()
            .withUpdateResourceValidator((x, y) -> ConnectorResponse.error(ERROR_CODE, ERROR_MSG))
            .build();

    // when
    ConnectorResponse response = handler.updateResource(id, RAW_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode(ERROR_CODE).hasMessage(ERROR_MSG);
  }

  @Test
  void shouldNotUpdateResourceWhenPreCallbackReturnsError() {
    // given
    String id = randomId();
    var handler =
        handlerBuilder()
            .withPreUpdateResourceCallback((x, y) -> ConnectorResponse.error(ERROR_CODE, ERROR_MSG))
            .build();

    // when
    ConnectorResponse response = handler.updateResource(id, RAW_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode(ERROR_CODE).hasMessage(ERROR_MSG);
  }

  @Test
  void shouldReturnErrorWhenPostCallbackReturnsError() {
    // given
    resourceIngestionDefinitionRepository.save(VARIANT_RESOURCE);
    var handler =
        handlerBuilder()
            .withPostUpdateResourceCallback(
                (x, y) -> ConnectorResponse.error(ERROR_CODE, ERROR_MSG))
            .build();

    // when
    ConnectorResponse response =
        handler.updateResource(RESOURCE_INGESTION_DEFINITION_ID, RAW_INGESTION_CONFIGURATIONS);

    // then
    assertThat(response).hasResponseCode(ERROR_CODE).hasMessage(ERROR_MSG);
  }

  private UpdateResourceHandlerTestBuilder handlerBuilder() {
    return new UpdateResourceHandlerTestBuilder()
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withTransactionManager(new InMemoryTransactionManager())
        .withIngestionProcessRepository(ingestionProcessRepository)
        .withResourceIngestionDefinitionRepository(resourceIngestionDefinitionRepository)
        .withUpdateResourceValidator((x, y) -> ConnectorResponse.success())
        .withPreUpdateResourceCallback((x, y) -> ConnectorResponse.success())
        .withPostUpdateResourceCallback((x, y) -> ConnectorResponse.success());
  }

  public void createResource(
      String resourceIngestionDefinitionId,
      String name,
      boolean enabled,
      Variant resourceId,
      Variant resourceMetadata,
      List<IngestionConfiguration<Variant, Variant>> ingestionConfigurations) {
    this.resourceIngestionDefinitionRepository.save(
        new VariantResource(
            resourceIngestionDefinitionId,
            name,
            enabled,
            resourceId,
            resourceMetadata,
            ingestionConfigurations));
    if (enabled) {
      ingestionConfigurations.forEach(
          it ->
              ingestionProcessRepository.createProcess(
                  resourceIngestionDefinitionId,
                  it.getId(),
                  "DEFAULT",
                  SCHEDULED,
                  resourceMetadata));
    }
  }

  private void assertCurrentIngestionProcessesAreExactly(
      String resourceIngestionDefinitionId, List<IngestionProcess> processesBeforeUpdate) {
    var processes = ingestionProcessRepository.fetchAll(resourceIngestionDefinitionId);
    assertThat(processes).containsAll(processesBeforeUpdate);
  }

  private void assertIngestionConfigurationsAreEqual(
      String resourceIngestionDefinitionId, Variant updatedIngestionConfigurations) {
    var updatedResource =
        resourceIngestionDefinitionRepository.fetch(resourceIngestionDefinitionId);
    assertThat(updatedResource.orElseThrow())
        .hasIngestionConfigurations(
            IngestionConfigurationMapper.map(updatedIngestionConfigurations));
  }

  private void assertIngestionProcessExists(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String status) {
    var foundProcesses =
        ingestionProcessRepository.fetchAll(List.of(RESOURCE_INGESTION_DEFINITION_ID)).stream()
            .filter(
                process ->
                    process.getResourceIngestionDefinitionId().equals(resourceIngestionDefinitionId)
                        && process.getIngestionConfigurationId().equals(ingestionConfigurationId)
                        && process.getStatus().equals(status))
            .count();
    assertThat(foundProcesses).isEqualTo(1);
  }
}
