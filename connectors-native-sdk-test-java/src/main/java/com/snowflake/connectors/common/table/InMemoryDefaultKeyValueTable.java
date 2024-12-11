/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.util.sql.MergeStatementValidator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** In memory implementation of {@link KeyValueTable}. */
public class InMemoryDefaultKeyValueTable implements KeyValueTable {

  private static final int EXPRESSION_LIMIT = 16384;

  private final Map<String, FakeKeyValue> repository = new HashMap<>();
  private Predicate<Entry<String, FakeKeyValue>> getAllWherePredicate;

  @Override
  public Variant fetch(String key) {
    return Optional.ofNullable(repository.get(key))
        .orElseThrow(() -> new KeyNotFoundException(key))
        .getValue();
  }

  @Override
  public Map<String, Variant> fetchAll() {
    return repository.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, map -> map.getValue().getValue()));
  }

  @Override
  public void update(String key, Variant value) {
    requireNonNull(key, "Key must not be null");
    requireNonNull(value, "Value must not be null");

    repository.put(key, new FakeKeyValue(value, Instant.now()));
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    requireNonNull(getAllWherePredicate, "Define predicate before calling method getAllWhere");

    return repository.entrySet().stream()
        .filter(getAllWherePredicate)
        .map(Entry::getValue)
        .map(FakeKeyValue::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Sets the predicate used by {@link #getAllWhere(Column)} method.
   *
   * @param getAllWherePredicate new predicate
   */
  public void setGetAllWherePredicate(Predicate<Entry<String, FakeKeyValue>> getAllWherePredicate) {
    this.getAllWherePredicate = getAllWherePredicate;
  }

  @Override
  public void updateMany(List<String> ids, String fieldName, Variant fieldValue) {
    ids.forEach(
        id -> {
          var row = repository.get(id);
          var map = row.getValue().asMap();
          map.put(fieldName, fieldValue);
          row.setValue(new Variant(map));
          row.setUpdatedAt(Instant.now());
        });
  }

  @Override
  public void updateAll(List<KeyValue> keyValues) {
    if (keyValues.isEmpty()) {
      return;
    }

    MergeStatementValidator.validateRecordLimit(keyValues);

    for (KeyValue keyValue : keyValues) {
      update(keyValue.key(), keyValue.value());
    }
  }

  @Override
  public void delete(String key) {
    repository.remove(key);
  }

  /**
   * Returns the map backing this table.
   *
   * @return map backing this table
   */
  public Map<String, FakeKeyValue> getRepository() {
    return repository;
  }

  /** Clears this table. */
  public void clear() {
    repository.clear();
  }
}
