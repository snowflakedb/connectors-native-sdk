/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.object.Identifier;

/** Exception thrown when the specified warehouse is already used by the connector. */
public class WarehouseAlreadyUsedException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "WAREHOUSE_ALREADY_USED";

  private static final String MESSAGE = "Warehouse %s is already used by the application";

  /**
   * Creates a new {@link WarehouseAlreadyUsedException}.
   *
   * @param warehouse warehouse name
   */
  public WarehouseAlreadyUsedException(Identifier warehouse) {
    super(RESPONSE_CODE, format(MESSAGE, warehouse.getValue()));
  }
}
