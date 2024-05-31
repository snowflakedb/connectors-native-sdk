/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status.exception;

import com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus;
import com.snowflake.connectors.common.exception.ConnectorException;
import java.util.Arrays;

/** Exception thrown when an invalid connector configuration status is encountered. */
public class InvalidConnectorConfigurationStatusException extends ConnectorException {

  /** Error code of the exception, used in the underlying {@link #getResponse() response}. */
  public static final String RESPONSE_CODE = "INVALID_CONNECTOR_CONFIGURATION_STATUS";

  private static final String MESSAGE =
      "Invalid connector configuration status. "
          + "Expected one of statuses: %s. Current status: %s.";

  /**
   * Creates a new {@link InvalidConnectorStatusException}.
   *
   * @param actualState current connector configuration status
   * @param expectedStates expected connector configuration statuses
   */
  public InvalidConnectorConfigurationStatusException(
      ConnectorConfigurationStatus actualState, ConnectorConfigurationStatus... expectedStates) {
    super(RESPONSE_CODE, String.format(MESSAGE, Arrays.toString(expectedStates), actualState));
  }
}
