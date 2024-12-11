/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.configurationRepository;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link TestConfig}. */
public class TestConfigAssert extends AbstractAssert<TestConfigAssert, TestConfig> {

  /**
   * Creates a new {@link TestConfigAssert}.
   *
   * @param testConfig asserted test config
   * @param selfType self type
   */
  public TestConfigAssert(TestConfig testConfig, Class<TestConfigAssert> selfType) {
    super(testConfig, selfType);
  }

  /**
   * Asserts that this object is an instance of {@link TestConfig}.
   *
   * @return this assertion
   */
  public TestConfigAssert isTestConfigObject() {
    assertThat(actual.getClass())
        .withFailMessage(
            "Expected class to be <%s> but was <%s>", TestConfig.class, actual.getClass())
        .isEqualTo(TestConfig.class);
    return this;
  }

  /**
   * Asserts that this config has a flag equal to the specified value.
   *
   * @param flag expected flag
   * @return this assertion
   */
  public TestConfigAssert hasFlag(boolean flag) {
    assertThat(actual.getFlag())
        .withFailMessage("Expected flag to be <%s> but was <%s>", flag, actual.getFlag())
        .isEqualTo(flag);
    return this;
  }

  /**
   * Asserts that this config has a string config equal to the specified value.
   *
   * @param stringConfig expected string config
   * @return this assertion
   */
  public TestConfigAssert hasStringConfig(String stringConfig) {
    assertThat(actual.getStringConfig())
        .withFailMessage(
            "Expected stringConfig to be <%s> but was <%s>", stringConfig, actual.getStringConfig())
        .isEqualTo(stringConfig);
    return this;
  }

  /**
   * Asserts that this config has an int config equal to the specified value.
   *
   * @param intConfig expected int config
   * @return this assertion
   */
  public TestConfigAssert hasIntConfig(int intConfig) {
    assertThat(actual.getIntConfig())
        .withFailMessage(
            "Expected intConfig to be <%s> but was <%s>", intConfig, actual.getIntConfig())
        .isEqualTo(intConfig);
    return this;
  }
}
