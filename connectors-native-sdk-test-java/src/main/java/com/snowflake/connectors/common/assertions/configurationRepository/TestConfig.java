/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.configurationRepository;

/** Test implementation of a connector configuration entry. */
public class TestConfig {

  private boolean flag;
  private String stringConfig;
  private int intConfig;

  TestConfig() {}

  public TestConfig(boolean flag, String stringConfig, int intConfig) {
    this.flag = flag;
    this.stringConfig = stringConfig;
    this.intConfig = intConfig;
  }

  public boolean getFlag() {
    return flag;
  }

  public String getStringConfig() {
    return stringConfig;
  }

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
