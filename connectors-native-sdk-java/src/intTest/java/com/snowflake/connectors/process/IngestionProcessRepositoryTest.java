/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.process;

import static com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository.EXPRESSION_LIMIT;
import static com.snowflake.connectors.common.IdGenerator.randomId;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.stream.Collectors;
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
            "INITIAL",
            metadata);

    // then
    var result = repository.fetch(id);
    assertThat(result)
        .isPresent()
        .hasValueSatisfying(
            value ->
                assertThat(value)
                    .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
                    .hasIngestionConfigurationId(ingestionConfigurationId)
                    .hasType("DEFAULT")
                    .hasStatus("INITIAL")
                    .hasFinishedAtNull(true)
                    .hasMetadata(metadata));
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
        .hasValueSatisfying(
            value ->
                assertThat(value)
                    .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
                    .hasIngestionConfigurationId(ingestionConfigurationId)
                    .hasType("DEFAULT")
                    .hasStatus("READY_TO_INGEST")
                    .hasFinishedAtNull(true)
                    .hasMetadata(null));
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
    assertTrue(result.isPresent());
    assertThat(result.get())
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
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "FINISHED", null);

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
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "FINISHED", null);

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
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);

    // when
    repository.endProcess(id);

    // then
    var result = repository.fetch(id);
    assertTrue(result.isPresent());
    assertThat(result.get())
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus("FINISHED")
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
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);
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
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "INITIAL", null);

    // when
    repository.endProcess(resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT");

    // then
    var result = repository.fetch(id);
    assertTrue(result.isPresent());

    assertThat(result.get())
        .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
        .hasIngestionConfigurationId(ingestionConfigurationId)
        .hasType("DEFAULT")
        .hasStatus("FINISHED")
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
        resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "FINISHED", null);

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
            resourceIngestionDefinitionId, ingestionConfigurationId, "DEFAULT", "FINISHED", null);

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
                .collect(Collectors.toList()));
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
        .hasValueSatisfying(value -> assertThat(value).hasFinishedAtNull(true));
    assertThat(repository.fetch(processWithNotNull.getId()))
        .isPresent()
        .hasValueSatisfying(value -> assertThat(value).hasFinishedAt(DATETIME));
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
            "SCHEDULED",
            now,
            null,
            metadata1);
    var processToUpdate =
        new IngestionProcess(
            processId,
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            type,
            "COMPLETED",
            now,
            now,
            metadata2);

    // when
    repository.save(processToInsert);

    // then
    var insertResult = repository.fetch(processId);
    assertThat(insertResult)
        .isPresent()
        .hasValueSatisfying(
            value ->
                assertThat(value)
                    .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
                    .hasIngestionConfigurationId(ingestionConfigurationId)
                    .hasType(type)
                    .hasStatus("SCHEDULED")
                    .hasCreatedAt(now)
                    .hasFinishedAtNull(true)
                    .hasMetadata(metadata1));

    // when
    repository.save(processToUpdate);

    // then
    var updateResult = repository.fetch(processId);
    assertThat(updateResult)
        .isPresent()
        .hasValueSatisfying(
            value ->
                assertThat(value)
                    .hasResourceIngestionDefinitionId(resourceIngestionDefinitionId)
                    .hasIngestionConfigurationId(ingestionConfigurationId)
                    .hasType(type)
                    .hasStatus("COMPLETED")
                    .hasCreatedAt(now)
                    .hasFinishedAt(now)
                    .hasMetadata(metadata2));
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

  private static IngestionProcess ingestionProcessWithId(String id) {
    return new IngestionProcess(
        id,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        "RERUN",
        "INITIAL",
        DATETIME,
        DATETIME,
        new Variant(Map.of("key", "value")));
  }

  private static IngestionProcess ingestionProcessWithRandomId() {
    return ingestionProcessWithId(randomId());
  }
}
