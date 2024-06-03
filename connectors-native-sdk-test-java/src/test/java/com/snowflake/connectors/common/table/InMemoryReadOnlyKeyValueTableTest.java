/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.snowpark_java.types.Variant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InMemoryReadOnlyKeyValueTableTest {

  InMemoryReadOnlyKeyValueTable table =
      new InMemoryReadOnlyKeyValueTable(
          Map.of(
              "java_library", new Variant("Connectors Java SDK"),
              "py_library", new Variant("Connectors Python SDK")));

  @Test
  void shouldFetchValueForAKey() {
    // then
    assertThat(table.fetch("java_library")).isEqualTo(new Variant("Connectors Java SDK"));
  }

  @Test
  void shouldThrowKeyNotFoundExceptionWhenKeyDoesNotExist() {
    // then
    assertThatExceptionOfType(KeyNotFoundException.class)
        .isThrownBy(() -> table.fetch("non-existing-key"));
  }

  @Test
  void shouldFetchAllValues() {
    // when
    List<Variant> values = new ArrayList<>(table.fetchAll().values());

    // then
    assertThat(values)
        .containsExactlyInAnyOrder(
            new Variant("Connectors Java SDK"), new Variant("Connectors Python SDK"));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateValue() {
    // then
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> table.update("key", new Variant("value")));
  }

  @Test
  void shouldThrowNullPointerExceptionWhenPredicatesAreNotSet() {
    // then
    assertThatThrownBy(() -> table.getAllWhere(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Define predicate before calling method getAllWhere");
  }

  @Test
  void shouldFetchAllValuesForAFilter() {
    // given
    table.setGetAllWherePredicate(it -> it.getKey().contains("java"));

    // then
    assertThat(table.getAllWhere(null)).isEqualTo(List.of(new Variant("Connectors Java SDK")));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateManyValues() {
    // then
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> table.updateMany(List.of("id"), "key", new Variant("value")));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToUpdateAllValues() {
    // then
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> table.updateAll(List.of(new KeyValue("id", new Variant("value")))));
  }

  @Test
  void shouldThrowUnsupportedOperationExceptionWhenTryingToDeleteValue() {
    // then
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> table.delete("key"));
  }
}
