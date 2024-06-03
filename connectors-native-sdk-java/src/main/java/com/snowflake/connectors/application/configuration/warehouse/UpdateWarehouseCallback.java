/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Callback called during the {@link UpdateWarehouseHandler} execution, may be used to provide
 * custom warehouse update logic.
 *
 * <p>Default implementation of this callback calls the {@code PUBLIC.UPDATE_WAREHOUSE_INTERNAL}
 * procedure.
 */
@FunctionalInterface
public interface UpdateWarehouseCallback {

  /**
   * Executes logic using the provided warehouse.
   *
   * @param warehouse new warehouse name provided to the handler
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse execute(Identifier warehouse);
}
