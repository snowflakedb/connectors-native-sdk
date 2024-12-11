/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.assertions.application.status;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import org.assertj.core.api.AbstractAssert;

/** AssertJ based assertions for {@link FullConnectorStatus}. */
public class FullConnectorStatusAssert
    extends AbstractAssert<FullConnectorStatusAssert, FullConnectorStatus> {

  /**
   * Creates a new {@link FullConnectorStatusAssert}.
   *
   * @param fullConnectorStatus asserted full connector status
   * @param selfType self type
   */
  public FullConnectorStatusAssert(
      FullConnectorStatus fullConnectorStatus, Class<FullConnectorStatusAssert> selfType) {
    super(fullConnectorStatus, selfType);
  }

  /**
   * Asserts that this full status has a connector status equal to the specified value.
   *
   * @param status expected connector status
   * @return this assertion
   */
  public FullConnectorStatusAssert isInStatus(ConnectorStatus status) {
    assertThat(actual.getStatus()).isEqualTo(status);
    return this;
  }

  /**
   * Asserts that this full status has a connector configuration status equal to the specified
   * value.
   *
   * @param status expected connector configuration status
   * @return this assertion
   */
  public FullConnectorStatusAssert isInConfigurationStatus(ConnectorConfigurationStatus status) {
    assertThat(actual.getConfigurationStatus()).isEqualTo(status);
    return this;
  }
}
