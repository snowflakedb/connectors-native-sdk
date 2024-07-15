/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.enable;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.INGESTION_PROCESS;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.RESOURCE_INGESTION_DEFINITION;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.application.ingestion.definition.InMemoryResourceIngestionDefinitionRepository;
import com.snowflake.connectors.application.ingestion.definition.IngestionConfiguration;
import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.ingestion.process.InMemoryIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.InMemoryTransactionManager;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class EnableResourceHandlerTest {

  private static final Variant METADATA = new Variant("example");

  InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  InMemoryResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository = new InMemoryResourceIngestionDefinitionRepository<>();

  @Test
  void shouldSwitchEnabledFlagAndStartIngestionProcesses() {
    // given
    String configurationId1 = "id1";
    String configurationId2 = "id2";
    String definitionId =
        resourceIngestionDefinitionExists(false, List.of(configurationId1, configurationId2));
    ingestionProcessExists(FINISHED, definitionId, configurationId1);
    ingestionProcessExists(FINISHED, definitionId, configurationId2);
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertThatNewIngestionProcessWasCreated(definitionId, configurationId1, METADATA);
    assertThatNewIngestionProcessWasCreated(definitionId, configurationId2, METADATA);
  }

  @Test
  void shouldCreateNewIngestionProcessButNotCopyMetadataWhenFinishedProcessDoesNotExist() {
    // given
    String configurationId1 = "id1";
    String definitionId = resourceIngestionDefinitionExists(false, List.of(configurationId1));
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertThatNewIngestionProcessWasCreated(definitionId, configurationId1, null);
  }

  @Test
  void shouldReturnErrorWhenResourceDoesNotExist() {
    // given
    String id = randomId();
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.enableResource(id);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with resourceId '" + id + "' does not exist");
  }

  @Test
  void shouldDoNothingWhenResourceIsAlreadyEnabled() {
    // given
    String configurationId = randomId();
    String definitionId = resourceIngestionDefinitionExists(true, List.of(configurationId));
    ingestionProcessExists(FINISHED, definitionId, configurationId);
    String existingProcessId = ingestionProcessExists("IN_PROGRESS", definitionId, configurationId);
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertNewProcessHasNotBeenCreated(definitionId, configurationId, existingProcessId);
  }

  @Test
  void shouldNotEnableResourceWhenPreCallbackReturnsError() {
    // given
    String configurationId = randomId();
    String definitionId = resourceIngestionDefinitionExists(false, List.of(configurationId));
    ingestionProcessExists(FINISHED, definitionId, configurationId);
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder()
            .withPreEnableResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionIsDisabled(definitionId);
    assertNewProcessHasNotBeenCreated(definitionId, configurationId);
  }

  @Test
  void shouldNotEnableResourceWhenValidatorReturnsError() {
    // given
    String configurationId = randomId();
    String definitionId = resourceIngestionDefinitionExists(false, List.of(configurationId));
    ingestionProcessExists(FINISHED, definitionId, configurationId);
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder()
            .withEnableResourceValidator(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionIsDisabled(definitionId);
    assertNewProcessHasNotBeenCreated(definitionId, configurationId);
  }

  @Test
  void shouldReturnErrorWhenPostCallbackReturnsError() {
    // given
    String configurationId = randomId();
    String definitionId = resourceIngestionDefinitionExists(false, List.of(configurationId));
    ingestionProcessExists(FINISHED, definitionId, configurationId);
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder()
            .withPostEnableResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response = handler.enableResource(definitionId);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertThatNewIngestionProcessWasCreated(definitionId, configurationId, METADATA);
  }

  private EnableResourceHandlerTestBuilder handlerBuilder() {
    return new EnableResourceHandlerTestBuilder()
        .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
        .withTransactionManager(new InMemoryTransactionManager())
        .withIngestionProcessRepository(ingestionProcessRepository)
        .withResourceIngestionDefinitionRepository(resourceIngestionDefinitionRepository)
        .withEnableResourceValidator(x -> ConnectorResponse.success())
        .withPreEnableResourceCallback(x -> ConnectorResponse.success())
        .withPostEnableResourceCallback(x -> ConnectorResponse.success());
  }

  private String resourceIngestionDefinitionExists(
      boolean enabled, List<String> ingestionConfigurationIds) {
    String id = randomId();
    var ingestionConfigurations =
        ingestionConfigurationIds.stream()
            .map(this::ingestionConfiguration)
            .collect(Collectors.toList());
    var definition =
        new VariantResource(
            id,
            "name",
            enabled,
            new Variant("resource_id"),
            new Variant(Map.of("aa", "dd", "bb", "eee")),
            ingestionConfigurations);
    resourceIngestionDefinitionRepository.save(definition);
    return id;
  }

  private IngestionConfiguration<Variant, Variant> ingestionConfiguration(String id) {
    return new IngestionConfiguration<>(
        id, INCREMENTAL, new Variant(Map.of("aa", "dd")), INTERVAL, "10m", new Variant("example"));
  }

  private String ingestionProcessExists(
      String status, String resourceIngestionDefinitionId, String configurationId) {
    String id = randomId();
    var process =
        new IngestionProcess(
            id,
            resourceIngestionDefinitionId,
            configurationId,
            "DEFAULT",
            status,
            Instant.now(),
            Instant.now(),
            METADATA);
    ingestionProcessRepository.save(process);
    return id;
  }

  private void assertResourceIngestionDefinitionIsEnabled(String resourceIngestionDefinitionId) {
    var definition = resourceIngestionDefinitionRepository.fetch(resourceIngestionDefinitionId);
    assertThat(definition).isPresent().get(RESOURCE_INGESTION_DEFINITION).isEnabled();
  }

  private void assertResourceIngestionDefinitionIsDisabled(String resourceIngestionDefinitionId) {
    var definition = resourceIngestionDefinitionRepository.fetch(resourceIngestionDefinitionId);
    assertThat(definition).isPresent().get(RESOURCE_INGESTION_DEFINITION).isDisabled();
  }

  private void assertThatNewIngestionProcessWasCreated(
      String definitionId, String configurationId, Variant expectedMetadata) {
    List<IngestionProcess> fetched =
        ingestionProcessRepository.fetchAll(definitionId, configurationId, "DEFAULT").stream()
            .filter(process -> SCHEDULED.equals(process.getStatus()))
            .collect(Collectors.toList());
    assertThat(fetched).hasSize(1).singleElement(INGESTION_PROCESS).hasMetadata(expectedMetadata);
  }

  private void assertNewProcessHasNotBeenCreated(
      String definitionId, String configurationId, String... excludedProcesses) {
    List<String> excluded = Arrays.asList(excludedProcesses);
    List<IngestionProcess> newProcesses =
        ingestionProcessRepository.fetchAll(definitionId, configurationId, "DEFAULT").stream()
            .filter(process -> !FINISHED.equals(process.getStatus()))
            .filter(process -> !excluded.contains(process.getId()))
            .collect(Collectors.toList());
    assertThat(newProcesses).isEmpty();
  }
}
