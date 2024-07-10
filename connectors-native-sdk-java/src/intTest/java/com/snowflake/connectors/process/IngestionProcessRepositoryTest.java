/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.process;

import static com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository.EXPRESSION_LIMIT;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.INGESTION_PROCESS;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.application.ingestion.process.IngestionProcessUpdateException;
import com.snowflake.connectors.common.table.DuplicateKeyException;
import com.snowflake.connectors.common.table.RecordsLimitExceededException;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class IngestionProcessRepositoryTest extends BaseIntegrationTest {

  private static final Instant DATETIME =
      LocalDateTime.of(2024, 7, 1, 9, 43).toInstant(ZoneOffset.UTC);

  DefaultIngestionProcessRepository repository = new DefaultIngestionProcessRepository(session);

  @Test
  void shouldInsertAndFetchIngestionProcess() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var metadata = new Variant(Map.of("key", "value"));

    // when
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            "DEFAULT",
            SCHEDULED,
            metadata);

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus(SCHEDULED)
        .hasFinishedAtNull(true)
        .hasMetadata(metadata);
  }

  @Test
  void shouldUpdateIngestionProcessById() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);

    // when
    repository.updateStatus(id, "READY_TO_INGEST");

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus("READY_TO_INGEST")
        .hasFinishedAtNull(true)
        .hasMetadata(null);
  }

  @Test
  void shouldThrowWhenUpdatingIngestionProcessByIdThatDoesNotExist() {

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () -> repository.updateStatus(UUID.randomUUID().toString(), "READY_TO_INGEST"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldThrowWhenUpdatingIngestionProcessByIdAndMoreThanOneRowWasUpdated() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);
    session
        .sql(
            String.format(
                "INSERT INTO state.ingestion_process(id, resource_ingestion_definition_id, "
                    + "ingestion_configuration_id, type, status) VALUES ('%s', '%s', "
                    + "'%s', 'DEFAULT', 'INITIAL')",
                id, resourceIngestionDefinitionId, ingestionConfigurationId))
        .collect();

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () -> repository.updateStatus(id, "READY_TO_INGEST"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 2");
  }

  @Test
  void
      shouldUpdateIngestionProcessByResourceIngestionDefinitionIdIngestionConfigurationIdAndType() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);

    // when
    repository.updateStatus(
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "READY_TO_INGEST");

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus("READY_TO_INGEST")
        .hasFinishedAtNull(true)
        .hasMetadata(null);
  }

  @Test
  void
      shouldThrowWhenUpdatingIngestionProcessByIngestionDefinitionIdIngestionConfigurationIdAndTypeThatDoNotExist() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () ->
                repository.updateStatus(
                    resourceIngestionDefinitionId,
                    ingestionConfigurationId,
                    "DEFAULT",
                    "READY_TO_INGEST"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void
      shouldThrowWhenNoUnfinishedIngestionProcessesArePresentWhenUpdatingAProcessByResourceIngestionDefinitionId() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    repository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", FINISHED, null);

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () ->
                repository.updateStatus(
                    resourceIngestionDefinitionId,
                    ingestionConfigurationId,
                    "DEFAULT",
                    "NEW_STATUS"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldThrowWhenNoUnfinishedIngestionProcessesArePresentWhenUpdatingAProcessById() {

    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", FINISHED, null);

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class, () -> repository.updateStatus(id, "NEW_STATUS"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldEndIngestionProcessById() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", SCHEDULED, null);

    // when
    repository.endProcess(id);

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus(FINISHED)
        .hasFinishedAtNull(false)
        .hasMetadata(null);
  }

  @Test
  void shouldThrowWhenEndingIngestionProcessByIdThatDoesNotExist() {
    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () -> repository.endProcess(UUID.randomUUID().toString()));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldThrowWhenEndingIngestionProcessByIdAndMoreThanOneRowWasUpdated() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", SCHEDULED, null);
    session
        .sql(
            String.format(
                "INSERT INTO state.ingestion_process(id, resource_ingestion_definition_id,"
                    + " ingestion_configuration_id, type, status) VALUES ('%s', '%s', '%s',"
                    + " 'DEFAULT', 'INITIAL')",
                id, resourceIngestionDefinitionId, ingestionConfigurationId))
        .collect();

    // then
    var ex = assertThrows(IngestionProcessUpdateException.class, () -> repository.endProcess(id));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 2");
  }

  @Test
  void shouldEndIngestionProcessByResourceIngestionDefinitionIdIngestionConfigurationIdAndType() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", SCHEDULED, null);

    // when
    repository.endProcess(resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT");

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus(FINISHED)
        .hasFinishedAtNull(false)
        .hasMetadata(null);
  }

  @Test
  void
      shouldThrowWhenEndingIngestionProcessByIngestionDefinitionIdIngestionConfigurationIdAndTypeThatDoNotExist() {

    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () ->
                repository.endProcess(
                    resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void
      shouldThrowWhenNoUnfinishedIngestionProcessesArePresentWhenEndingAProcessByResourceIngestionDefinitionId() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    repository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", FINISHED, null);

    // then
    var ex =
        assertThrows(
            IngestionProcessUpdateException.class,
            () ->
                repository.endProcess(
                    resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT"));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldThrowWhenNoUnfinishedIngestionProcessesArePresentWhenEndingAProcessById() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    var id =
        repository.createProcess(
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", FINISHED, null);

    // then
    var ex = assertThrows(IngestionProcessUpdateException.class, () -> repository.endProcess(id));
    assertEquals(
        ex.getMessage(),
        "Precisely 1 row should be updated in ingestion_process. Number of updated rows: 0");
  }

  @Test
  void shouldFetchAllRecordsByResourceIngestionDefinitionIdIngestionConfigurationIdAndType() {
    // given
    var resourceIngestionDefinitionId = UUID.randomUUID().toString();
    var ingestionConfigurationId = UUID.randomUUID().toString();
    repository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);
    repository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "RERUN", "READY_TO_INGEST", null);
    repository.createProcess(
        resourceIngestionDefinitionId, ingestionConfigurationId, "RERUN", "INITIAL", null);

    // when
    var result =
        repository.fetchAll(resourceIngestionDefinitionId, ingestionConfigurationId, "RERUN");

    // then
    assertEquals(result.size(), 2);
    assertThat(result.get(0))
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("RERUN")
        .hasStatus("INITIAL")
        .hasFinishedAtNull(true)
        .hasMetadata(null);
    assertThat(result.get(1))
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("RERUN")
        .hasStatus("READY_TO_INGEST")
        .hasFinishedAtNull(true)
        .hasMetadata(null);
  }

  @Test
  void shouldSaveAllIngestionProcesses() {
    // given
    var ingestionProcesses = new ArrayList<IngestionProcess>();
    for (int i = 0; i < 10; i++) {
      ingestionProcesses.add(ingestionProcessWithRandomId());
    }

    // when
    repository.save(ingestionProcesses);

    // then
    var result =
        repository.fetchAll(
            ingestionProcesses.stream()
                .map(IngestionProcess::getResourceIngestionDefinitionId)
                .collect(toList()));
    ingestionProcesses.forEach(
        expectedIngestionProcess -> {
          var actual =
              result.stream()
                  .filter(obj -> Objects.equals(obj.getId(), expectedIngestionProcess.getId()))
                  .findFirst()
                  .orElseThrow();
          assertThat(actual)
              .hasResourceIngestionDefinitionId(
                  expectedIngestionProcess.getResourceIngestionDefinitionId())
              .hasIngestionConfigurationId(expectedIngestionProcess.getIngestionConfigurationId())
              .hasType(expectedIngestionProcess.getType())
              .hasStatus(expectedIngestionProcess.getStatus())
              .hasFinishedAt(expectedIngestionProcess.getCreatedAt())
              .hasCreatedAt(expectedIngestionProcess.getFinishedAt())
              .hasMetadata(expectedIngestionProcess.getMetadata());
        });
  }

  @Test
  void shouldSaveIngestionProcessesWithFinishedAtNullAndNotNull() {
    // given
    var processWithNull = ingestionProcessWithRandomId().withFinishedAt(null);
    var processWithNotNull = ingestionProcessWithRandomId().withFinishedAt(DATETIME);
    var ingestionProcesses = List.of(processWithNotNull, processWithNull);

    // when
    repository.save(ingestionProcesses);

    // then
    assertThat(repository.fetch(processWithNull.getId()))
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasFinishedAtNull(true);
    assertThat(repository.fetch(processWithNotNull.getId()))
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasFinishedAt(DATETIME);
  }

  @Test
  void shouldThrowWhileSavingTooMuchIngestionProcesses() {
    // given
    var ingestionProcesses = new ArrayList<IngestionProcess>();
    for (int i = 0; i <= EXPRESSION_LIMIT; i++) {
      ingestionProcesses.add(ingestionProcessWithRandomId());
    }

    // then
    assertThrows(RecordsLimitExceededException.class, () -> repository.save(ingestionProcesses));
  }

  @Test
  void shouldInsertAndUpdateIngestionProcessUsingSaveMethod() {
    // given
    var processId = randomId();
    var resourceIngestionDefinitionId = randomId();
    var ingestionConfigurationId = randomId();
    var metadata1 = new Variant(Map.of("key", "value"));
    var metadata2 = new Variant(Map.of("key2", "value2"));
    var now = Instant.now();
    var type = "DEFAULT";

    var processToInsert =
        new IngestionProcess(
            processId,
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            type,
            SCHEDULED,
            now,
            null,
            metadata1);
    var processToUpdate =
        new IngestionProcess(
            processId,
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            type,
            FINISHED,
            now,
            now,
            metadata2);

    // when
    repository.save(processToInsert);

    // then
    var insertResult = repository.fetch(processId);
    assertThat(insertResult)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType(type)
        .hasStatus(SCHEDULED)
        .hasCreatedAt(now)
        .hasFinishedAtNull(true)
        .hasMetadata(metadata1);

    // when
    repository.save(processToUpdate);

    // then
    var updateResult = repository.fetch(processId);
    assertThat(updateResult)
        .isPresent()
        .get(INGESTION_PROCESS)
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType(type)
        .hasStatus(FINISHED)
        .hasCreatedAt(now)
        .hasFinishedAt(now)
        .hasMetadata(metadata2);
  }

  @Test
  void shouldFailToUpdateManyFieldsWhenKeysAreDuplicated() {
    // given
    List<IngestionProcess> processes =
        List.of(
            ingestionProcessWithId("one"),
            ingestionProcessWithId("one"),
            ingestionProcessWithRandomId());

    // expect
    assertThatExceptionOfType(DuplicateKeyException.class)
        .isThrownBy(() -> repository.save(processes))
        .withMessage("There were duplicated keys in the collection. Duplicated IDs found [one].");
  }

  @Test
  void shouldFetchAllActiveProcesses() {
    // given
    String definitionId = randomId();
    String scheduledProcessId = ingestionProcessExists(SCHEDULED, definitionId);
    String inProgressProcessId = ingestionProcessExists(IN_PROGRESS, definitionId);
    String completedProcessId = ingestionProcessExists(FINISHED, definitionId);
    String scheduledProcess2Id = ingestionProcessExists(SCHEDULED, randomId());

    // when
    List<IngestionProcess> processes = repository.fetchAllActive(definitionId);

    // then
    List<String> processIds = processes.stream().map(IngestionProcess::getId).collect(toList());
    assertThat(processIds).containsExactlyInAnyOrder(scheduledProcessId, inProgressProcessId);
  }

  @Test
  void shouldFetchLastFinishedProcess() {
    // given
    Instant finishedAt1 = Instant.now();
    Instant finishedAt2 = finishedAt1.plusSeconds(10);
    String definitionId = randomId();
    String configurationId = randomId();
    ingestionProcessExists(FINISHED, definitionId, configurationId, finishedAt1);
    String lastProcessId =
        ingestionProcessExists(FINISHED, definitionId, configurationId, finishedAt2);

    // when
    var result = repository.fetchLastFinished(definitionId, configurationId, "DEFAULT");

    // then
    assertThat(result).isPresent().get(INGESTION_PROCESS).hasId(lastProcessId);
  }

  @Test
  void shouldReturnEmptyWhenLastFinishedProcessDoesNotExist() {
    // given
    String definitionId = randomId();
    String configurationId = randomId();

    // when
    var result = repository.fetchLastFinished(definitionId, configurationId, "DEFAULT");

    // then
    assertThat(result).isEmpty();
  }

  private String ingestionProcessExists(String status, String definitionId) {
    String id = randomId();
    var ingestionProcess = ingestionProcess(id, status, definitionId, randomId());
    repository.save(ingestionProcess);
    return id;
  }

  private String ingestionProcessExists(
      String status, String definitionId, String configurationId, Instant finishedAt) {
    String id = randomId();
    var ingestionProcess =
        ingestionProcess(id, status, definitionId, configurationId).withFinishedAt(finishedAt);
    repository.save(ingestionProcess);
    return id;
  }

  private static IngestionProcess ingestionProcessWithId(String id) {
    return ingestionProcess(id, SCHEDULED, randomId(), randomId());
  }

  private static IngestionProcess ingestionProcess(
      String id, String status, String resourceIngestionDefinitionId, String configurationId) {
    return new IngestionProcess(
        id,
        resourceIngestionDefinitionId,
        configurationId,
        "DEFAULT",
        status,
        DATETIME,
        DATETIME,
        new Variant(Map.of("key", "value")));
  }

  private static IngestionProcess ingestionProcessWithRandomId() {
    return ingestionProcessWithId(randomId());
  }
}
