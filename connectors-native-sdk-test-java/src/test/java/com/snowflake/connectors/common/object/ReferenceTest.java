/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ReferenceTest {

  @ParameterizedTest
  @CsvSource({
    "reference('REF'),REF",
    "reference('#AFWAFAV\"_99'),#AFWAFAV\"_99",
    "reference('\"TABLE\"'),\"TABLE\""
  })
  void validFullReferenceTest(String reference, String expectedReferenceName) {
    assertThat(Reference.validate(reference)).isTrue();
    assertThat(Reference.of(reference))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly(reference, expectedReferenceName);
  }

  @Test
  void createReferenceNameTest() {
    String referenceName = "reference";
    assertThat(Reference.from(referenceName))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly("reference('reference')", referenceName);
  }

  @Test
  void invalidReferenceTest() {
    String reference = "reference";
    assertThat(Reference.validate(reference)).isFalse();
    assertThatExceptionOfType(InvalidReferenceException.class)
        .isThrownBy(() -> Reference.of(reference));
  }

  @Test
  void invalidReferenceNameTest() {
    String reference = "reference('REF')";
    assertThat(Reference.validate(reference)).isTrue();
    assertThatExceptionOfType(InvalidReferenceException.class)
        .isThrownBy(() -> Reference.from(reference));
  }

  @Test
  void emptyReferenceTest() {
    assertThat(Reference.of(""))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly("reference('')", null);
  }

  @Test
  void emptyReferenceNameTest() {
    assertThat(Reference.from(""))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly("reference('')", null);
  }

  @Test
  void nullReferenceTest() {
    assertThat(Reference.of(null))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly("reference('')", null);
  }

  @Test
  void nullReferenceNameTest() {
    assertThat(Reference.from(null))
        .extracting(Reference::value, Reference::referenceName)
        .containsExactly("reference('')", null);
  }

  @Test
  void nullValidateTest() {
    assertThat(Reference.validate(null)).isFalse();
  }
}
