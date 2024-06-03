/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.table.FakeKeyValue.FakeKeyValueComparator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.types.Variant;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** In memory implementation of {@link AppendOnlyTable}. */
public class InMemoryAppendOnlyKeyValueTable implements AppendOnlyTable {

  private final Map<String, Collection<FakeKeyValue>> repository = new HashMap<>();

  private Predicate<Entry<String, Collection<FakeKeyValue>>> getAllWherePredicate;

  @Override
  public Variant fetch(String key) {
    return Optional.ofNullable(repository.get(key))
        .orElseThrow(() -> new KeyNotFoundException(key))
        .stream()
        .min(new FakeKeyValueComparator())
        .map(FakeKeyValue::getValue)
        .orElseThrow();
  }

  @Override
  public void insert(String key, Variant value) {
    requireNonNull(key, "Key must not be null");
    requireNonNull(value, "Value must not be null");
    var timestamp = Instant.now();
    var row = new FakeKeyValue(value, timestamp);
    if (repository.containsKey(key)) {
      repository.get(key).add(row);
    } else {
      repository.put(key, new ArrayList<>(Collections.singletonList(row)));
    }
  }

  @Override
  public List<Variant> getAllWhere(Column filter) {
    requireNonNull(getAllWherePredicate, "Define predicate before calling method getAllWhere");

    return repository.entrySet().stream()
        .filter(getAllWherePredicate)
        .map(Entry::getValue)
        .map(it -> it.stream().min(new FakeKeyValueComparator()).orElseThrow())
        .map(FakeKeyValue::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Sets the predicate used by the {@link #getAllWhere(Column) getAllWhere} method.
   *
   * @param getAllWherePredicate new predicate
   */
  public void setGetAllWherePredicate(
      Predicate<Entry<String, Collection<FakeKeyValue>>> getAllWherePredicate) {
    this.getAllWherePredicate = getAllWherePredicate;
  }
}
