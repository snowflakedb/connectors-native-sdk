/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.common.response;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.response.ConnectorResponse;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link ConnectorResponse}. */
public class ConnectorResponseAssert
    extends AbstractAssert<ConnectorResponseAssert, ConnectorResponse> {

  public ConnectorResponseAssert(
      ConnectorResponse connectorResponse, Class<ConnectorResponseAssert> selfType) {
    super(connectorResponse, selfType);
  }

  /**
   * Asserts that this connector response has a response code equal to {@code OK}.
   *
   * @return this assertion
   */
  public ConnectorResponseAssert hasOKResponseCode() {
    hasResponseCode("OK");
    return this;
  }

  /**
   * Asserts that this connector response has a response code equal to the specified value.
   *
   * @param code expected response code
   * @return this assertion
   */
  public ConnectorResponseAssert hasResponseCode(String code) {
    assertThat(actual.getResponseCode()).isEqualTo(code);
    return this;
  }

  /**
   * Asserts that this connector response has a message equal to the specified value.
   *
   * @param message expected response message
   * @return this assertion
   */
  public ConnectorResponseAssert hasMessage(String message) {
    assertThat(actual.getMessage()).isEqualTo(message);
    return this;
  }
}
