/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.exception.InvalidConnectorStatusException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.RequiredPrivilegesMissingException;
import com.snowflake.snowpark_java.Session;
import java.util.function.Supplier;

/** A service for basic management of the connector lifecycle processes. */
public interface LifecycleService {

  /** Response code signifying a rollback operation has occurred */
  String ROLLBACK_CODE = "ROLLBACK";

  /** Response code signifying an unknown, unrecoverable error has occurred */
  String UNKNOWN_ERROR_CODE = "UNKNOWN_ERROR";

  /**
   * Validates if the current connector status contains provided ones.
   *
   * @param statuses expected connector statuses
   * @throws InvalidConnectorStatusException when the current connector status is different from the
   *     expected ones
   */
  void validateStatus(ConnectorStatus... statuses);

  /**
   * Updates the connector status to a provided one.
   *
   * @param status new connector status
   */
  void updateStatus(ConnectorStatus status);

  /**
   * Validates whether the application instance has been granted all privileges necessary to run the
   * lifecycle processes.
   *
   * @throws RequiredPrivilegesMissingException if any required privilege has not been granted
   */
  void validateRequiredPrivileges();

  /**
   * Executes the specified action with handling of rollback and unexpected errors.
   *
   * <p>If a {@link #ROLLBACK_CODE ROLLBACK} code was returned by the action - the connector status
   * is updated accordingly and the action response is returned by this method.
   *
   * <p>If an unexpected exception has been thrown during the action execution - a response with an
   * {@link #UNKNOWN_ERROR_CODE UNKNOWN_ERROR} code is returned by this method.
   *
   * @param action an action to execute
   * @return a response with the code {@code OK} if the execution was successful, a response with
   *     the code {@link #ROLLBACK_CODE ROLLBACK} if the action performed a rollback operation, or a
   *     response with an {@link #UNKNOWN_ERROR_CODE UNKNOWN_ERROR} code and an error message
   */
  ConnectorResponse withRollbackHandling(Supplier<ConnectorResponse> action);

  /**
   * Returns a new instance of the default service implementation.
   *
   * <p>Default implementation of the service uses a default implementation of {@link
   * ConnectorStatusService}.
   *
   * @param session Snowpark session object
   * @param statusAfterRollback Connector status set if a rollback operation has occurred
   * @return a new service instance
   */
  static LifecycleService getInstance(Session session, ConnectorStatus statusAfterRollback) {
    var statusService = ConnectorStatusService.getInstance(session);
    return new DefaultLifecycleService(session, statusService, statusAfterRollback);
  }
}
