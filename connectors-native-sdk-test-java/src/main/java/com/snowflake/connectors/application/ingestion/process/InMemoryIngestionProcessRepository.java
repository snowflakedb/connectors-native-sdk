/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.FINISHED;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;

import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * In memory implementation of {@link IngestionProcessRepository} and {@link
 * CrudIngestionProcessRepository}
 */
public class InMemoryIngestionProcessRepository
    implements IngestionProcessRepository, CrudIngestionProcessRepository {

  private final Map<String, IngestionProcess> repository = new HashMap<>();

  @Override
  public String createProcess(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status,
      Variant metadata) {
    var id = UUID.randomUUID().toString();
    var ingestionProcess =
        new IngestionProcess(
            id,
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            type,
            status,
            Instant.now(),
            null,
            metadata);
    repository.put(id, ingestionProcess);
    return id;
  }

  @Override
  public void updateStatus(String processId, String status) {
    IngestionProcess ingestionProcess = repository.get(processId);
    if (ingestionProcess == null) {
      return;
    }
    IngestionProcess newProcess =
        new IngestionProcess(
            ingestionProcess.getId(),
            ingestionProcess.getResourceIngestionDefinitionId(),
            ingestionProcess.getIngestionConfigurationId(),
            ingestionProcess.getType(),
            status,
            ingestionProcess.getCreatedAt(),
            ingestionProcess.getFinishedAt(),
            ingestionProcess.getMetadata());
    repository.replace(processId, newProcess);
  }

  @Override
  public void updateStatus(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endProcess(String processId) {
    IngestionProcess process =
        fetch(processId).orElseThrow(() -> new IngestionProcessUpdateException(0L));
    IngestionProcess updatedProcess = process.withStatus(FINISHED).withFinishedAt(Instant.now());
    repository.put(processId, updatedProcess);
  }

  @Override
  public void endProcess(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<IngestionProcess> fetch(String processId) {
    return Optional.ofNullable(repository.get(processId));
  }

  @Override
  public Optional<IngestionProcess> fetchLastFinished(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    return repository.values().stream()
        .filter(
            process ->
                resourceIngestionDefinitionId.equals(process.getResourceIngestionDefinitionId()))
        .filter(process -> ingestionConfigurationId.equals(process.getIngestionConfigurationId()))
        .filter(process -> type.equals(process.getType()))
        .filter(process -> FINISHED.equals(process.getStatus()))
        .max(Comparator.comparing(IngestionProcess::getFinishedAt));
  }

  @Override
  public List<IngestionProcess> fetchAll(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    return repository.values().stream()
        .filter(
            process ->
                resourceIngestionDefinitionId.equals(process.getResourceIngestionDefinitionId()))
        .filter(process -> ingestionConfigurationId.equals(process.getIngestionConfigurationId()))
        .filter(process -> type.equals(process.getType()))
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionProcess> fetchAll(List<String> resourceIngestionDefinitionIds) {
    return repository.values().stream()
        .filter(
            process ->
                resourceIngestionDefinitionIds.contains(process.getResourceIngestionDefinitionId()))
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionProcess> fetchAll(String status) {
    return repository.values().stream()
        .filter(process -> status.equals(process.getStatus()))
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionProcess> fetchAllActive(String resourceIngestionDefinitionId) {
    List<String> statuses = List.of(SCHEDULED, IN_PROGRESS);
    return repository.values().stream()
        .filter(
            process ->
                resourceIngestionDefinitionId.equals(process.getResourceIngestionDefinitionId()))
        .filter(process -> statuses.contains(process.getStatus()))
        .collect(Collectors.toList());
  }

  @Override
  public void save(IngestionProcess ingestionProcess) {
    repository.put(ingestionProcess.getId(), ingestionProcess);
  }

  @Override
  public void save(Collection<IngestionProcess> ingestionProcesses) {
    ingestionProcesses.forEach(process -> repository.put(process.getId(), process));
  }

  /** Clears the repository. */
  public void clear() {
    repository.clear();
  }

  /**
   * Returns repository.
   *
   * @return collection for repository
   */
  public Map<String, IngestionProcess> getRepository() {
    return repository;
  }
}
