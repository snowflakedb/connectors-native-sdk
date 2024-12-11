/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.variant.VariantMapper.mapToVariant;
import static com.snowflake.snowpark_java.Functions.col;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DefaultKeyValueTableTest extends BaseIntegrationTest {

  private KeyValueTable table;

  @BeforeAll
  void beforeAll() {
    table = new DefaultKeyValueTable(session, "STATE.APP_CONFIG");
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
    assertThatThrownBy(() -> table.fetch("key-3"))
        .isInstanceOf(KeyNotFoundException.class)
        .hasMessage("Key not found in configuration: key-3");
  }

  @Test
  void shouldUpdateManyFields() {
    // given
    List<KeyValue> rows =
        List.of(
            new KeyValue("key-44", new Variant(16)),
            new KeyValue("key-45", new Variant(17)),
            new KeyValue("key-46", new Variant(18)),
            new KeyValue("key-47", new Variant(19)));

    // when
    table.updateAll(rows);

    // then
    List<Variant> results = table.getAllWhere(col("key").like(lit("key-4%")));
    assertThat(results)
        .hasSize(4)
        .containsExactlyInAnyOrder(
            rows.stream().map(element -> mapToVariant(element.value())).toArray(Variant[]::new));
  }

  @Test
  void shouldFailToUpdateManyFieldsWhenKeysAreDuplicated() {
    // given
    List<KeyValue> rows =
        List.of(
            new KeyValue("key-44", new Variant(16)),
            new KeyValue("key-45", new Variant(17)),
            new KeyValue("key-44", new Variant(19)));

    // expect
    assertThatExceptionOfType(DuplicateKeyException.class)
        .isThrownBy(() -> table.updateAll(rows))
        .withMessage(
            "There were duplicated keys in the collection. Duplicated IDs found [key-44].");
  }

  @Test
  void shouldInsertAndUpdate16384Rows() {
    // given
    List<KeyValue> rowsToCreate =
        IntStream.range(1, 16385)
            .mapToObj(number -> new KeyValue("k-" + number, new Variant(number)))
            .collect(Collectors.toList());
    List<KeyValue> rowsToUpdate =
        IntStream.range(1, 16385)
            .mapToObj(number -> new KeyValue("k-" + number, new Variant(2 * number)))
            .collect(Collectors.toList());

    // when
    table.updateAll(rowsToCreate);

    // then
    List<Variant> results = table.getAllWhere(col("key").like(lit("k-%")));
    assertThat(results)
        .containsExactlyInAnyOrder(
            rowsToCreate.stream()
                .map(element -> mapToVariant(element.value()))
                .toArray(Variant[]::new));

    // when
    table.updateAll(rowsToUpdate);

    // then
    List<Variant> updatedResult = table.getAllWhere(col("key").like(lit("k-%")));
    assertThat(updatedResult)
        .containsExactlyInAnyOrder(
            rowsToUpdate.stream()
                .map(element -> mapToVariant(element.value()))
                .toArray(Variant[]::new));
  }

  @Test
  void shouldFailToUpdateMoreValuesThanExpressionLimit_16384() {
    // given
    List<KeyValue> rows =
        IntStream.range(1, 16386)
            .mapToObj(number -> new KeyValue("k-" + number, new Variant(number)))
            .collect(Collectors.toList());

    // expect
    assertThatThrownBy(() -> table.updateAll(rows))
        .isInstanceOf(RecordsLimitExceededException.class)
        .hasMessage(
            "Limit of singular update for many properties is: 16384. Reduce amount of passed"
                + " keyValues or split them into chunks smaller or equal to: 16384");
  }
}
