/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static java.lang.String.format;

import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.object.Identifier;

/** Exception thrown when the specified warehouse is inaccessible to the application. */
public class InaccessibleWarehouseException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INACCESSIBLE_WAREHOUSE";

  private static final String MESSAGE = "Warehouse %s is inaccessible to the application";

  /**
   * Creates a new {@link InaccessibleWarehouseException}.
   *
   * @param warehouse warehouse name
   */
  public InaccessibleWarehouseException(Identifier warehouse) {
    super(RESPONSE_CODE, format(MESSAGE, warehouse.toSqlString()));
  }
}
