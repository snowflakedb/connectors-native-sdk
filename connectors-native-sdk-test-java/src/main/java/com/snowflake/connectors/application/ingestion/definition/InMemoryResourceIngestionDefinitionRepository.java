/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.snowflake.snowpark_java.Column;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** In memory implementation of {@link ResourceIngestionDefinitionRepository} */
public class InMemoryResourceIngestionDefinitionRepository<
        R extends ResourceIngestionDefinition<?, ?, ?, ?>>
    implements ResourceIngestionDefinitionRepository<R> {

  private final Map<String, R> repository;

  private Predicate<Entry<String, R>> fetchAllWherePredicate;

  /** Creates a new {@link InMemoryResourceIngestionDefinitionRepository}, backed by a hashmap. */
  public InMemoryResourceIngestionDefinitionRepository() {
    this.repository = new HashMap<>();
  }

  @Override
  public Optional<R> fetch(String id) {
    return Optional.ofNullable(repository.get(id));
  }

  @Override
  public Optional<R> fetchByResourceId(Object resourceId) {
    requireNonNull(resourceId);
    return repository.values().stream()
        .filter(resource -> resourceId.equals(resource.getResourceId()))
        .findAny();
  }

  @Override
  public List<R> fetchAllById(List<String> ids) {
    return repository.values().stream().filter(r -> ids.contains(r.getId())).collect(toList());
  }

  @Override
  public List<R> fetchAllEnabled() {
    return repository.values().stream()
        .filter(ResourceIngestionDefinition::isEnabled)
        .collect(toList());
  }

  @Override
  public List<R> fetchAllWhere(Column condition) {
    requireNonNull(fetchAllWherePredicate, "Define predicate before calling method fetchAllWhere");

    return repository.entrySet().stream()
        .filter(fetchAllWherePredicate)
        .map(Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<R> fetchAll() {
    return new ArrayList<>(repository.values());
  }

  /**
   * Sets the predicate used by the {@link #fetchAllWhere(Column) fetchAllWhere} method.
   *
   * @param fetchAllWherePredicate new predicate
   */
  public void setFetchAllWherePredicate(Predicate<Entry<String, R>> fetchAllWherePredicate) {
    this.fetchAllWherePredicate = fetchAllWherePredicate;
  }

  @Override
  public long countEnabled() {
    return fetchAllEnabled().size();
  }

  @Override
  public void save(R resource) {
    repository.put(resource.getId(), resource);
  }

  @Override
  public void saveMany(List<R> resources) {
    if (resources.isEmpty()) {
      return;
    }
    resources.forEach(this::save);
  }

  /** Clears the repository. */
  public void clear() {
    repository.clear();
  }
}
