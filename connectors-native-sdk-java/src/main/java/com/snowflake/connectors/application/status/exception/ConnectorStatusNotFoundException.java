/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status.exception;

import com.snowflake.connectors.common.exception.ConnectorException;

/** Exception thrown when no connector status data was found in the database. */
public class ConnectorStatusNotFoundException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "CONNECTOR_STATUS_NOT_FOUND";

  private static final String MESSAGE = "Connector status record does not exist in database.";

  /** Creates a new {@link ConnectorStatusNotFoundException}. */
  public ConnectorStatusNotFoundException() {
    super(RESPONSE_CODE, MESSAGE);
  }
}
