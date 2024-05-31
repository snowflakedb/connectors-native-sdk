/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status.exception;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.common.exception.ConnectorException;
import java.util.Arrays;

/** Exception thrown when an invalid connector status is encountered. */
public class InvalidConnectorStatusException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_CONNECTOR_STATUS";

  private static final String MESSAGE =
      "Invalid connector status. Expected status: %s. Current status: %s.";

  /**
   * Creates a new {@link InvalidConnectorStatusException}.
   *
   * @param currentStatus current connector status
   * @param expectedStatuses expected connector statuses
   */
  public InvalidConnectorStatusException(
      ConnectorStatus currentStatus, ConnectorStatus... expectedStatuses) {
    super(RESPONSE_CODE, String.format(MESSAGE, Arrays.toString(expectedStatuses), currentStatus));
  }
}
