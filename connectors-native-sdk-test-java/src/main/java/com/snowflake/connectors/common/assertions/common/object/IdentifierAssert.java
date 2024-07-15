/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.object;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import org.assertj.core.api.AbstractObjectAssert;

/** AssertJ based assertions for {@link Identifier}. */
public class IdentifierAssert extends AbstractObjectAssert<IdentifierAssert, Identifier> {

  public IdentifierAssert(Identifier identifier, Class<IdentifierAssert> selfType) {
    super(identifier, selfType);
  }

  /**
   * Asserts that this identifier has a value equal to the specified value.
   *
   * @param value expected value
   * @return this assertion
   */
  public IdentifierAssert hasValue(String value) {
    assertThat(actual.getValue()).isEqualTo(value);
    return this;
  }

  /**
   * Asserts that this identifier has an unquoted value equal to the specified value.
   *
   * @param unquotedValue expected unquoted value
   * @return this assertion
   */
  public IdentifierAssert hasUnquotedValue(String unquotedValue) {
    assertThat(actual.getUnquotedValue()).isEqualTo(unquotedValue);
    return this;
  }

  /**
   * Asserts that this identifier has a quoted value equal to the specified value.
   *
   * @param quotedValue expected quoted value
   * @return this assertion
   */
  public IdentifierAssert hasQuotedValue(String quotedValue) {
    assertThat(actual.getQuotedValue()).isEqualTo(quotedValue);
    return this;
  }

  /**
   * Asserts that this identifier is unquoted.
   *
   * @return this assertion
   */
  public IdentifierAssert isUnquoted() {
    assertThat(actual.isUnquoted()).isTrue();
    assertThat(actual.isQuoted()).isFalse();
    return this;
  }

  /**
   * Asserts that this identifier is quoted.
   *
   * @return this assertion
   */
  public IdentifierAssert isQuoted() {
    assertThat(actual.isUnquoted()).isFalse();
    assertThat(actual.isQuoted()).isTrue();
    return this;
  }
}
