/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.configurationRepository;

/** Test implementation of a connector configuration entry. */
public class TestConfig {

  private boolean flag;
  private String stringConfig;
  private int intConfig;

  TestConfig() {}

  /**
   * Creates a new {@link TestConfig}.
   *
   * @param flag boolean value
   * @param stringConfig string value
   * @param intConfig int value
   */
  public TestConfig(boolean flag, String stringConfig, int intConfig) {
    this.flag = flag;
    this.stringConfig = stringConfig;
    this.intConfig = intConfig;
  }

  /**
   * Returns the test boolean value.
   *
   * @return test boolean value
   */
  public boolean getFlag() {
    return flag;
  }

  /**
   * Returns the test string value.
   *
   * @return test string value
   */
  public String getStringConfig() {
    return stringConfig;
  }

  /**
   * Returns the test int value.
   *
   * @return test int value
   */
  public int getIntConfig() {
    return intConfig;
  }

  public int hashCode() {
    int result;
    result = (flag ? 1 : 0);
    result = 31 * result + (stringConfig != null ? stringConfig.hashCode() : 0);
    result = 31 * result + intConfig;
    return result;
  }
}
