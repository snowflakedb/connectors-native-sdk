/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.common.object.Identifier.AutoQuoting;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class IdentifierTest {

  @ParameterizedTest
  @MethodSource("validUnquotedIdentifiersWithoutAutoQuoting")
  void shouldCreateValidUnquotedIdentifiersWithoutAutoQuoting(
      String raw, String value, String unquotedValue, String quotedValue) {
    // expect
    assertThat(Identifier.isValid(raw)).isTrue();

    // and
    assertThat(Identifier.from(raw))
        .hasValue(value)
        .hasUnquotedValue(unquotedValue)
        .hasQuotedValue(quotedValue)
        .isUnquoted();
  }

  @ParameterizedTest
  @MethodSource("validQuotedIdentifiersWithoutAutoQuoting")
  void shouldCreateValidQuotedIdentifiersWithoutAutoQuoting(
      String raw, String value, String unquotedValue, String quotedValue) {
    // expect
    assertThat(Identifier.isValid(raw)).isTrue();

    // and
    assertThat(Identifier.from(raw))
        .hasValue(value)
        .hasUnquotedValue(unquotedValue)
        .hasQuotedValue(quotedValue)
        .isQuoted();
  }

  @ParameterizedTest
  @MethodSource("invalidIdentifiersWithoutAutoQuoting")
  void shouldThrowErrorOnInvalidIdentifiersWithoutAutoQuoting(String raw) {
    // expect
    assertThat(Identifier.isValid(raw)).isFalse();
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(raw))
        .withMessage(format("'%s' is not a valid Snowflake identifier", raw));

    // and
    assertThat(Identifier.isValid(raw)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("validUnquotedIdentifiersWithAutoQuoting")
  void shouldCreateValidUnquotedIdentifiersWithAutoQuoting(
      String raw, String value, String unquotedValue, String quotedValue) {
    // expect
    assertThat(Identifier.from(raw, AutoQuoting.ENABLED))
        .hasValue(value)
        .hasUnquotedValue(unquotedValue)
        .hasQuotedValue(quotedValue)
        .isUnquoted();
  }

  @ParameterizedTest
  @MethodSource("validQuotedIdentifiersWithAutoQuoting")
  void shouldCreateValidQuotedIdentifiersWithAutoQuoting(
      String raw, String value, String unquotedValue, String quotedValue) {
    // expect
    assertThat(Identifier.from(raw, AutoQuoting.ENABLED))
        .hasValue(value)
        .hasUnquotedValue(unquotedValue)
        .hasQuotedValue(quotedValue)
        .isQuoted();
  }

  @Test
  void shouldThrowErrorOnNullIdentifier() {
    // expect
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(null))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(null, AutoQuoting.DISABLED))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(null, AutoQuoting.ENABLED))
        .withMessage("'null' is not a valid Snowflake identifier");

    // and
    assertThat(Identifier.isValid(null)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("unquotedIdentifiers")
  void shouldReturnIdentifierIsUnquoted(String identifier) {
    // expect
    assertThat(Identifier.isUnquoted(identifier)).isTrue();
    assertThat(Identifier.isQuoted(identifier)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("quotedIdentifiers")
  void shouldReturnIdentifierIsQuoted(String identifier) {
    // expect
    assertThat(Identifier.isUnquoted(identifier)).isFalse();
    assertThat(Identifier.isQuoted(identifier)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("validIdentifiers")
  void shouldReturnTheSameIdentifierFromVariant(String raw) {
    // expect
    var identifier = Identifier.from(raw);
    assertThat(identifier.getVariantValue().asString()).isEqualTo(raw);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void shouldAllowBlankIdentifiersOnlyIfQuoted(String raw) {
    // expect
    assertThatExceptionOfType(InvalidIdentifierException.class)
        .isThrownBy(() -> Identifier.from(raw, AutoQuoting.DISABLED))
        .withMessage(format("'%s' is not a valid Snowflake identifier", raw));

    // and
    assertThat(Identifier.from(format("\"%s\"", raw)))
        .hasQuotedValue(format("\"%s\"", raw))
        .hasValue(format("\"%s\"", raw));

    // and
    assertThat(Identifier.from(raw, AutoQuoting.ENABLED))
        .hasQuotedValue(format("\"%s\"", raw))
        .hasValue(format("\"%s\"", raw));
  }

  private static Stream<Arguments> validUnquotedIdentifiersWithoutAutoQuoting() {
    return Stream.of(
        Arguments.of("abcd", "abcd", "abcd", "\"abcd\""),
        Arguments.of("AbCd", "AbCd", "AbCd", "\"AbCd\""),
        Arguments.of("ABCD", "ABCD", "ABCD", "\"ABCD\""),
        Arguments.of("A$c6", "A$c6", "A$c6", "\"A$c6\""));
  }

  private static Stream<Arguments> validQuotedIdentifiersWithoutAutoQuoting() {
    return Stream.of(
        Arguments.of("\"abcd\"", "\"abcd\"", "abcd", "\"abcd\""),
        Arguments.of("\"AbCd\"", "\"AbCd\"", "AbCd", "\"AbCd\""),
        Arguments.of("\"ABCD\"", "\"ABCD\"", "ABCD", "\"ABCD\""),
        Arguments.of("\"@bc6\"", "\"@bc6\"", "@bc6", "\"@bc6\""),
        Arguments.of("\"a_b-c$❄\"", "\"a_b-c$❄\"", "a_b-c$❄", "\"a_b-c$❄\""),
        Arguments.of("\"ab\"\"cd\"", "\"ab\"\"cd\"", "ab\"cd", "\"ab\"\"cd\""),
        Arguments.of("\"ab'cd\"", "\"ab'cd\"", "ab'cd", "\"ab'cd\""),
        Arguments.of("\"a.b.c.d\"", "\"a.b.c.d\"", "a.b.c.d", "\"a.b.c.d\""));
  }

  private static Stream<String> invalidIdentifiersWithoutAutoQuoting() {
    return Stream.of(null, "", "-", " ", "123", "@bc6", "a_b-c$❄", "ab\"\"cd", "a.b.c.d");
  }

  private static Stream<Arguments> validUnquotedIdentifiersWithAutoQuoting() {
    return Stream.of(
        Arguments.of("ABCD", "ABCD", "ABCD", "\"ABCD\""),
        Arguments.of("_ABCD$", "_ABCD$", "_ABCD$", "\"_ABCD$\""));
  }

  private static Stream<Arguments> validQuotedIdentifiersWithAutoQuoting() {
    return Stream.of(
        Arguments.of("abcd", "\"abcd\"", "abcd", "\"abcd\""),
        Arguments.of("AbCd", "\"AbCd\"", "AbCd", "\"AbCd\""),
        Arguments.of("A$c6", "\"A$c6\"", "A$c6", "\"A$c6\""),
        Arguments.of("@bc6", "\"@bc6\"", "@bc6", "\"@bc6\""),
        Arguments.of("a_b-c$❄", "\"a_b-c$❄\"", "a_b-c$❄", "\"a_b-c$❄\""),
        Arguments.of("ab\"cd", "\"ab\"\"cd\"", "ab\"cd", "\"ab\"\"cd\""),
        Arguments.of("ab'cd", "\"ab'cd\"", "ab'cd", "\"ab'cd\""),
        Arguments.of("a.b.c.d", "\"a.b.c.d\"", "a.b.c.d", "\"a.b.c.d\""),
        Arguments.of("\"abcd\"", "\"abcd\"", "abcd", "\"abcd\""),
        Arguments.of("\"AbCd\"", "\"AbCd\"", "AbCd", "\"AbCd\""),
        Arguments.of("\"ABCD\"", "\"ABCD\"", "ABCD", "\"ABCD\""),
        Arguments.of("\"@bc6\"", "\"@bc6\"", "@bc6", "\"@bc6\""),
        Arguments.of("\"a_b-c$❄\"", "\"a_b-c$❄\"", "a_b-c$❄", "\"a_b-c$❄\""),
        Arguments.of("\"ab\"\"cd\"", "\"ab\"\"cd\"", "ab\"cd", "\"ab\"\"cd\""),
        Arguments.of("\"ab'cd\"", "\"ab'cd\"", "ab'cd", "\"ab'cd\""),
        Arguments.of("\"a.b.c.d\"", "\"a.b.c.d\"", "a.b.c.d", "\"a.b.c.d\""));
  }

  private static Stream<String> unquotedIdentifiers() {
    return Stream.of("abcd", "AbCd", "ABCD", "A$c6", "_abcd");
  }

  private static Stream<String> quotedIdentifiers() {
    return Stream.of("\"abcd\"", "\"AbCd\"", "\"ABCD\"", "\"A$c6\"", "\"ab\"\"cd\"", "\"a_b-c$❄\"");
  }

  private static Stream<String> validIdentifiers() {
    return Stream.concat(unquotedIdentifiers(), quotedIdentifiers());
  }
}
