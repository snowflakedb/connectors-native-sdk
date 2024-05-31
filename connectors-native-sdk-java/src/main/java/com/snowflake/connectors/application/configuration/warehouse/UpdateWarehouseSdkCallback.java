/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Callback called during the {@link UpdateWarehouseHandler} execution, used to provide additional
 * logic for sdk-controlled components.
 *
 * <p>Default implementation of this callback:
 *
 * <ul>
 *   <li>updates the warehouse used by the default {@link
 *       com.snowflake.connectors.application.scheduler.Scheduler Scheduler} implementation
 *   <li>updates the warehouse used by Task Reactor instances
 * </ul>
 */
public interface UpdateWarehouseSdkCallback {

  /**
   * Executes logic using the provided warehouse.
   *
   * @param warehouse new warehouse name provided to the handler
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(Identifier warehouse);
}
