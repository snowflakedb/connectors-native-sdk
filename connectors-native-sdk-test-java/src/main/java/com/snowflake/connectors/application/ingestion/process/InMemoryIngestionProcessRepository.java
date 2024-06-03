/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Collection;
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
    throw new UnsupportedOperationException();
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
  public List<IngestionProcess> fetchAll(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IngestionProcess> fetchAll(List<String> resourceIngestionDefinitionIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IngestionProcess> fetchAll(String status) {
    return repository.values().stream()
        .filter(process -> status.equals(process.getStatus()))
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
}
