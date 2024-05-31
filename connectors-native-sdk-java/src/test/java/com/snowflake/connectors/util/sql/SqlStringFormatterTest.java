/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SqlStringFormatterTest {
  @Test
  void shouldQuoteString() {
    String input = "value";

    String result = SqlStringFormatter.quoted(input);

    assertThat(result).isEqualTo("\"value\"");
  }

  @ParameterizedTest
  @CsvSource({
    "SOME-STRING$#@,\"SOME-STRING$#@\"",
    "SOME\"STRING$#@,\"SOME\"\"STRING$#@\"",
    "\"SOME\"STRING$#@\",\"\"\"SOME\"\"STRING$#@\"\"\""
  })
  void shouldEscapeIdentifier(String identifier, String expected) {
    String result = SqlStringFormatter.escapeIdentifier(identifier);

    assertThat(result).isEqualTo(expected);
  }
}
