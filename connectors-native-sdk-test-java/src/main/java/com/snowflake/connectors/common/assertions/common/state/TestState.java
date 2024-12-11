/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.state;

/** Test implementation of a connector state entry. */
public class TestState {

  private boolean flag;
  private String stringState;
  private int intState;

  TestState() {}

  /**
   * Creates a new {@link TestState}.
   *
   * @param flag boolean value
   * @param stringConfig string value
   * @param intConfig int value
   */
  public TestState(boolean flag, String stringConfig, int intConfig) {
    this.flag = flag;
    this.stringState = stringConfig;
    this.intState = intConfig;
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
  public String getStringState() {
    return stringState;
  }

  /**
   * Returns the test int value.
   *
   * @return test int value
   */
  public int getIntState() {
    return intState;
  }
}
