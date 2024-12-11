/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability;

import static java.util.Objects.requireNonNull;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryIngestionRunRepository
    implements IngestionRunRepository, CrudIngestionRunRepository {
  private final Map<String, IngestionRun> repository = new HashMap<>();
  private Predicate<IngestionRun> findWherePredicate;
  private Predicate<IngestionRun> findByPredicate;
  private Comparator<IngestionRun> findByComparator;
  private Predicate<IngestionRun> findOngoingIngestionPredicate;

  @Override
  public String startRun(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String ingestionProcessId,
      Variant metadata) {
    String id = UUID.randomUUID().toString();
    IngestionRun ingestionRun =
        new IngestionRun(
            id,
            resourceIngestionDefinitionId,
            ingestionConfigurationId,
            IngestionRun.IngestionStatus.IN_PROGRESS,
            Instant.now(),
            null,
            0,
            Instant.now(),
            ingestionProcessId,
            metadata);
    repository.put(id, ingestionRun);
    return id;
  }

  @Override
  public void endRun(
      String id,
      IngestionRun.IngestionStatus ingestionStatus,
      Long ingestedRows,
      Mode mode,
      Variant variant) {
    IngestionRun ingestionRun = findById(id).orElseThrow();
    IngestionRun updated =
        new IngestionRun(
            id,
            ingestionRun.getIngestionDefinitionId(),
            ingestionRun.getIngestionConfigurationId(),
            ingestionStatus,
            ingestionRun.getStartedAt(),
            Instant.now(),
            ingestedRows,
            Instant.now(),
            ingestionRun.getIngestionProcessId(),
            variant);
    repository.put(id, updated);
  }

  @Override
  public void updateIngestedRows(String id, Long ingestedRows, Mode mode) {
    var ingestionRun = repository.get(id);
    var updatedIngestionRun =
        new IngestionRun(
            ingestionRun.getId(),
            ingestionRun.getIngestionDefinitionId(),
            ingestionRun.getIngestionConfigurationId(),
            ingestionRun.getStatus(),
            ingestionRun.getStartedAt(),
            ingestionRun.getCompletedAt(),
            ingestedRows,
            ingestionRun.getUpdatedAt(),
            ingestionRun.getIngestionProcessId(),
            ingestionRun.getMetadata());
    repository.replace(id, updatedIngestionRun);
  }

  @Override
  public Optional<IngestionRun> findById(String id) {
    return Optional.ofNullable(repository.get(id));
  }

  @Override
  public List<IngestionRun> fetchAllByResourceId(String id) {
    return repository.values().stream()
        .filter(value -> value.getIngestionDefinitionId().equals(id))
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionRun> fetchAllByProcessId(String id) {
    return repository.values().stream()
        .filter(value -> value.getIngestionProcessId().equals(id))
        .collect(Collectors.toList());
  }

  @Override
  public void deleteAllByResourceId(String resourceIngestionDefinitionId) {
    repository
        .values()
        .removeIf(value -> value.getIngestionDefinitionId().equals(resourceIngestionDefinitionId));
  }

  @Override
  public void save(IngestionRun ingestionRun) {
    repository.replace(ingestionRun.getId(), ingestionRun);
  }

  @Override
  public List<IngestionRun> findWhere(Column column) {
    requireNonNull(findWherePredicate, "Define findWherePredicate before calling method findWhere");

    return repository.values().stream().filter(findWherePredicate).collect(Collectors.toList());
  }

  @Override
  public Optional<IngestionRun> findBy(Column column, Column order) {
    requireNonNull(findByPredicate, "Define findByPredicate before calling method findBy");
    requireNonNull(findByComparator, "Define findByComparator before calling method findBy");

    return repository.values().stream().filter(findByPredicate).min(findByComparator);
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRuns() {
    return repository.values().stream()
        .filter(value -> value.getStatus() == IngestionRun.IngestionStatus.IN_PROGRESS)
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRuns(String resourceIngestionDefinitionId) {
    return repository.values().stream()
        .filter(value -> value.getIngestionDefinitionId().equals(resourceIngestionDefinitionId))
        .filter(value -> value.getStatus() == IngestionRun.IngestionStatus.IN_PROGRESS)
        .collect(Collectors.toList());
  }

  @Override
  public List<IngestionRun> findOngoingIngestionRunsWhere(Column column) {
    requireNonNull(
        findOngoingIngestionPredicate,
        "Define findOngoingIngestionPredicate before calling method findOngoingIngestionRunsWhere");

    return repository.values().stream()
        .filter(findOngoingIngestionPredicate)
        .filter(value -> value.getStatus() == IngestionRun.IngestionStatus.IN_PROGRESS)
        .collect(Collectors.toList());
  }

  public void clear() {
    repository.clear();
  }

  public void setFindWherePredicate(Predicate<IngestionRun> findWherePredicate) {
    this.findWherePredicate = findWherePredicate;
  }

  public void setFindByPredicate(Predicate<IngestionRun> findByPredicate) {
    this.findByPredicate = findByPredicate;
  }

  public void setFindByComparator(Comparator<IngestionRun> findByComparator) {
    this.findByComparator = findByComparator;
  }

  public void setFindOngoingIngestionPredicate(
      Predicate<IngestionRun> findOngoingIngestionPredicate) {
    this.findOngoingIngestionPredicate = findOngoingIngestionPredicate;
  }
}
