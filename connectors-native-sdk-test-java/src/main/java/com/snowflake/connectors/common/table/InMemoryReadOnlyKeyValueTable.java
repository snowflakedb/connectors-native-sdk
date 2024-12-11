/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.util.Objects.requireNonNull;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** In memory, read-only implementation of {@link KeyValueTable}. */
public class InMemoryReadOnlyKeyValueTable implements KeyValueTable {

  private final Map<String, Variant> repository;
  private Predicate<Entry<String, Variant>> getAllWherePredicate;

  /**
   * Creates a new {@link InMemoryReadOnlyKeyValueTable}.
   *
   * @param repository map backing this table
   */
  public InMemoryReadOnlyKeyValueTable(Map<String, Variant> repository) {
    this.repository = repository;
  }

  @Override
  public Variant fetch(String key) {
    return Optional.ofNullable(repository.get(key))
        .orElseThrow(() -> new KeyNotFoundException(key));
  }

  @Override
  public Map<String, Variant> fetchAll() {
    return repository;
  }

  @Override
  public void update(String key, Variant value) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    requireNonNull(getAllWherePredicate, "Define predicate before calling method getAllWhere");

    return repository.entrySet().stream()
        .filter(getAllWherePredicate)
        .map(Entry::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Sets the predicate used by {@link #getAllWhere(Column)} method.
   *
   * @param getAllWherePredicate new predicate
   */
  public void setGetAllWherePredicate(Predicate<Entry<String, Variant>> getAllWherePredicate) {
    this.getAllWherePredicate = getAllWherePredicate;
  }

  @Override
  public void updateMany(List<String> ids, String fieldName, Variant fieldValue) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public void updateAll(List<KeyValue> keyValues) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }

  @Override
  public void delete(String key) {
    throw new UnsupportedOperationException("ReadOnlyKeyValueTable does not allow to modify data!");
  }
}
