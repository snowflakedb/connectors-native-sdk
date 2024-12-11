/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link TestState}. */
public class TestStateAssert extends AbstractAssert<TestStateAssert, TestState> {

  /**
   * Creates a new {@link TestStateAssert}.
   *
   * @param testState asserted test state
   * @param selfType self type
   */
  public TestStateAssert(TestState testState, Class<TestStateAssert> selfType) {
    super(testState, selfType);
  }

  /**
   * Asserts that this object is an instance of {@link TestState}.
   *
   * @return this assertion
   */
  public TestStateAssert isTestStateObject() {
    assertThat(actual.getClass())
        .withFailMessage(
            "Expected class to be <%s> but was <%s>", TestState.class, actual.getClass())
        .isEqualTo(TestState.class);
    return this;
  }

  /**
   * Asserts that this state has a flag equal to the specified value.
   *
   * @param flag expected flag
   * @return this assertion
   */
  public TestStateAssert hasFlag(boolean flag) {
    assertThat(actual.getFlag())
        .withFailMessage("Expected flag to be <%s> but was <%s>", flag, actual.getFlag())
        .isEqualTo(flag);
    return this;
  }

  /**
   * Asserts that this state has a string state equal to the specified value.
   *
   * @param stringState expected string state
   * @return this assertion
   */
  public TestStateAssert hasStringState(String stringState) {
    assertThat(actual.getStringState())
        .withFailMessage(
            "Expected stringConfig to be <%s> but was <%s>", stringState, actual.getStringState())
        .isEqualTo(stringState);
    return this;
  }

  /**
   * Asserts that this state has an int state equal to the specified value.
   *
   * @param intState expected int state
   * @return this assertion
   */
  public TestStateAssert hasIntState(int intState) {
    assertThat(actual.getIntState())
        .withFailMessage(
            "Expected intConfig to be <%s> but was <%s>", intState, actual.getIntState())
        .isEqualTo(intState);
    return this;
  }
}
