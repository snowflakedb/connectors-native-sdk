/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.state;

/** Test implementation of a connector state entry. */
public class TestState {

  private boolean flag;
  private String stringState;
  private int intState;

  TestState() {}

  public TestState(boolean flag, String stringConfig, int intConfig) {
    this.flag = flag;
    this.stringState = stringConfig;
    this.intState = intConfig;
  }

  public boolean getFlag() {
    return flag;
  }

  public String getStringState() {
    return stringState;
  }

  public int getIntState() {
    return intState;
  }
}
