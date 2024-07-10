/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ReferenceTest {

  @ParameterizedTest
  @MethodSource("validReferences")
  void shouldCreateValidReferences(String referenceName, String reference) {
    // expect
    assertThat(Reference.isValid(reference)).isTrue();

    // and
    assertThat(Reference.of(reference)).hasName(referenceName).hasValue(reference);

    // and
    assertThat(Reference.from(referenceName)).hasName(referenceName).hasValue(reference);
  }

  @ParameterizedTest
  @MethodSource("invalidReferences")
  void shouldThrowErrorOnInvalidReferences(String reference) {
    // expect
    assertThat(Reference.isValid(reference)).isFalse();

    // and
    assertThatExceptionOfType(InvalidReferenceException.class)
        .isThrownBy(() -> Reference.of(reference))
        .withMessage(format("'%s' is not a valid Snowflake reference", reference));
  }

  @ParameterizedTest
  @MethodSource("invalidReferenceNames")
  void shouldThrowErrorOnInvalidReferenceNames(String referenceName) {
    // expect
    assertThat(Identifier.isUnquoted(referenceName)).isFalse();

    // and
    assertThatExceptionOfType(InvalidReferenceNameException.class)
        .isThrownBy(() -> Reference.from(referenceName))
        .withMessage(format("'%s' is not a valid name for a Snowflake reference", referenceName));
  }

  private static Stream<Arguments> validReferences() {
    return Stream.of(
        Arguments.of("reference", "reference('reference')"),
        Arguments.of("REF", "reference('REF')"),
        Arguments.of("AbCd_99$", "reference('AbCd_99$')"));
  }

  private static Stream<String> invalidReferences() {
    return Stream.of(
        null,
        "",
        "   ",
        "reference",
        "reference()",
        "reference('')",
        "reference('ab@cd')",
        "reference('\"abcd\"')");
  }

  private static Stream<String> invalidReferenceNames() {
    return Stream.of(null, "", "   ", "ab@cd", "\"ab@cd\"", "reference('')");
  }
}
