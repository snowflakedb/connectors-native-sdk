/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Sdk callback called during the {@link ResetConfigurationHandler} execution, may be used to
 * provide custom reset logic required by the sdk.
 *
 * <p>Default implementation of this callback executes the following statements in one transaction:
 *
 * <ul>
 *   <li>prerequisites update
 *   <li>connector configuration delete
 *   <li>connection configuration delete
 * </ul>
 */
@FunctionalInterface
public interface ResetConfigurationSdkCallback {

  /**
   * Executes custom sdk logic.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute();
}
