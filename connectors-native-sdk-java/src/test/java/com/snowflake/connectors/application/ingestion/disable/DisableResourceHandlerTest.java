/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.disable;

import static com.snowflake.connectors.application.ingestion.definition.IngestionStrategy.INCREMENTAL;
import static com.snowflake.connectors.application.ingestion.definition.ScheduleType.INTERVAL;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class DisableResourceHandlerTest {

  private static final String ID = "exampleId";

  InMemoryIngestionProcessRepository ingestionProcessRepository =
      new InMemoryIngestionProcessRepository();
  InMemoryResourceIngestionDefinitionRepository<VariantResource>
      resourceIngestionDefinitionRepository = new InMemoryResourceIngestionDefinitionRepository<>();

  @Test
  void shouldSwitchEnabledFlagAndEndAllActiveIngestionProcesses() {
    // given
    String definitionId = resourceIngestionDefinitionExists(true);
    String scheduledProcessId = ingestionProcessExists(SCHEDULED, definitionId);
    String inProgressProcessId = ingestionProcessExists(IN_PROGRESS, definitionId);
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.disableResource(definitionId);

    // then
    assertThat(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsDisabled(definitionId);
    assertIngestionProcessStatus(scheduledProcessId, FINISHED);
    assertIngestionProcessStatus(inProgressProcessId, FINISHED);
  }

  @Test
  void shouldReturnErrorWhenResourceDoesNotExist() {
    // given
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.disableResource(ID);

    // then
    assertThat(response)
        .hasResponseCode("INVALID_INPUT")
        .hasMessage("Resource with resourceId '" + ID + "' does not exist");
  }

  @Test
  void shouldDoNothingWhenResourceIsAlreadyDisabled() {
    // given
    String definitionId = resourceIngestionDefinitionExists(false);
    String scheduledProcessId = ingestionProcessExists(SCHEDULED, definitionId);
    var handler = handlerBuilder().build();

    // when
    ConnectorResponse response = handler.disableResource(definitionId);

    // then
    assertThat(response).hasOKResponseCode();
    assertResourceIngestionDefinitionIsDisabled(definitionId);
    assertIngestionProcessStatus(scheduledProcessId, SCHEDULED);
  }

  @Test
  void shouldNotDisableResourceWhenPreCallbackReturnsError() {
    // given
    String definitionId = resourceIngestionDefinitionExists(true);
    String scheduledProcessId = ingestionProcessExists(SCHEDULED, definitionId);
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder()
            .withPreDisableResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response = handler.disableResource(definitionId);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionIsEnabled(definitionId);
    assertIngestionProcessStatus(scheduledProcessId, SCHEDULED);
  }

  @Test
  void shouldReturnErrorWhenPostCallbackReturnsError() {
    // given
    String definitionId = resourceIngestionDefinitionExists(true);
    String scheduledProcessId = ingestionProcessExists(SCHEDULED, definitionId);
    String errorCode = "ERROR";
    String errorMessage = "error message";
    var handler =
        handlerBuilder()
            .withPostDisableResourceCallback(x -> ConnectorResponse.error(errorCode, errorMessage))
            .build();

    // when
    ConnectorResponse response = handler.disableResource(definitionId);

    // then
    assertThat(response).hasResponseCode(errorCode).hasMessage(errorMessage);
    assertResourceIngestionDefinitionIsDisabled(definitionId);
    assertIngestionProcessStatus(scheduledProcessId, FINISHED);
  }

  private DisableResourceHandlerTestBuilder handlerBuilder() {
    return (DisableResourceHandlerTestBuilder)
        new DisableResourceHandlerTestBuilder()
            .withErrorHelper(ConnectorErrorHelper.buildDefault(null, "TEST_SCOPE"))
            .withTransactionManager(new InMemoryTransactionManager())
            .withIngestionProcessRepository(ingestionProcessRepository)
            .withResourceIngestionDefinitionRepository(resourceIngestionDefinitionRepository)
            .withPreDisableResourceCallback(x -> ConnectorResponse.success())
            .withPostDisableResourceCallback(x -> ConnectorResponse.success());
  }

  private String resourceIngestionDefinitionExists(boolean enabled) {
    String id = randomId();
    var definition =
        new VariantResource(
            id,
            "name",
            enabled,
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
    resourceIngestionDefinitionRepository.save(definition);
    return id;
  }

  private String ingestionProcessExists(String status, String resourceIngestionDefinitionId) {
    String id = randomId();
    var process =
        new IngestionProcess(
            id,
            resourceIngestionDefinitionId,
            randomId(),
            "RERUN",
            status,
            Instant.now(),
            Instant.now(),
            new Variant(Map.of("key", "value")));
    ingestionProcessRepository.save(process);
    return id;
  }

  private void assertIngestionProcessStatus(String processId, String expectedStatus) {
    Optional<IngestionProcess> process = ingestionProcessRepository.fetch(processId);
    assertThat(process)
        .isPresent()
        .hasValueSatisfying(value -> assertThat(value).hasStatus(expectedStatus));
  }

  private void assertResourceIngestionDefinitionIsDisabled(String resourceIngestionDefinitionId) {
    var definition = resourceIngestionDefinitionRepository.fetch(resourceIngestionDefinitionId);
    assertThat(definition).isPresent().get(RESOURCE_INGESTION_DEFINITION).isDisabled();
  }

  private void assertResourceIngestionDefinitionIsEnabled(String resourceIngestionDefinitionId) {
    var definition = resourceIngestionDefinitionRepository.fetch(resourceIngestionDefinitionId);
    assertThat(definition).isPresent().get(RESOURCE_INGESTION_DEFINITION).isEnabled();
  }
}
