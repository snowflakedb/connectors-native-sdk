/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class IdentifierTest {

  @ParameterizedTest
  @MethodSource("validIdentifiers")
  void validIdentifierTest(String identifier) {
    assertThat(Identifier.validate(identifier)).isTrue();
    assertThat(Identifier.from(identifier)).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("invalidIdentifiers")
  void invalidIdentifierTest(String identifier) {
    assertThat(Identifier.validate(identifier)).isFalse();
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(identifier));
  }

  @ParameterizedTest
  @CsvSource({
    "val\"ue, \"val\"\"ue\"",
    "value, \"value\"",
    "VAL_99, \"VAL_99\"",
    "\"quoted\",\"quoted\"",
    "\"quo\"ted\",\"\"\"quo\"\"ted\"\"\""
  })
  void shouldAutoQuoteValue(String value, String expected) {
    var result = Identifier.fromWithAutoQuoting(value);
    assertThat(result.getName()).isEqualTo(value);
    assertThat(result.toSqlString()).isEqualTo(expected);
  }

  static Stream<String> validIdentifiers() {
    return Stream.of(
        "a", "table_name", "_azAz09$", "\"table name\"", "\"\"\"name\"\"\"", "\"n\"\"am\"\"e\"");
  }

  private static Stream<String> invalidIdentifiers() {
    return Stream.of("-", "  ", "1");
  }
}
