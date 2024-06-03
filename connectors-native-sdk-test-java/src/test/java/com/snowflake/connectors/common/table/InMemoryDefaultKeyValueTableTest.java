/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryDefaultKeyValueTableTest {

  InMemoryDefaultKeyValueTable table;

  @BeforeEach
  void setup() {
    table = new InMemoryDefaultKeyValueTable();
  }

  @Test
  void shouldAccessIntConfiguration() {
    // when
    table.update("key", new Variant(17));

    // then
    assertThat(table.fetch("key")).isEqualTo(new Variant(17));
  }

  @Test
  void shouldAccessStringConfiguration() {
    // when
    table.update("key2", new Variant("test value"));

    // then
    assertThat(table.fetch("key2")).isEqualTo(new Variant("test value"));
  }

  @Test
  void shouldDeleteStringConfiguration() {
    // given
    table.update("key-3", new Variant("deleted test value"));

    // when
    table.delete("key-3");

    // then
    assertThatExceptionOfType(KeyNotFoundException.class).isThrownBy(() -> table.fetch("key-3"));
  }

  @Test
  void shouldThrowNullPointerExceptionWhenPredicatesAreNotSet() {
    // then
    assertThatThrownBy(() -> table.getAllWhere(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Define predicate before calling method getAllWhere");
  }

  @Test
  void shouldUpdateManyField() {
    // given
    var rows =
        List.of(
            new KeyValue("key-44", new Variant(16)),
            new KeyValue("key-45", new Variant(17)),
            new KeyValue("key-46", new Variant(18)),
            new KeyValue("key-47", new Variant(19)));

    // when
    table.updateAll(rows);

    // then
    var results = table.fetchAll();
    assertThat(results.size()).isEqualTo(4);
  }

  @Test
  void shouldFailToUpdateMoreValuesThanExpressionLimit16384() {
    // given
    var rows =
        IntStream.range(1, 16386)
            .mapToObj(it -> new KeyValue("k-" + it, new Variant(it)))
            .collect(Collectors.toList());

    // then
    assertThatThrownBy(() -> table.updateAll(rows))
        .isInstanceOf(RecordsLimitExceededException.class)
        .hasMessage(
            "Limit of singular update for many properties is: 16384. Reduce amount of passed"
                + " keyValues or split them into chunks smaller or equal to: 16384");
  }
}
