/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle;

import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.application.status.ConnectorStatus.ERROR;

import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.PrivilegeTools;
import com.snowflake.snowpark_java.Session;
import java.util.function.Supplier;

/** Default implementation of {@link LifecycleService}. */
class DefaultLifecycleService implements LifecycleService {

  private static final String[] REQUIRED_PRIVILEGES = {"EXECUTE TASK"};

  private final Session session;
  private final ConnectorStatusService statusService;
  private final ConnectorStatus statusAfterRollback;

  DefaultLifecycleService(
      Session session, ConnectorStatusService statusService, ConnectorStatus statusAfterRollback) {
    this.session = session;
    this.statusService = statusService;
    this.statusAfterRollback = statusAfterRollback;
  }

  @Override
  public void validateStatus(ConnectorStatus... statuses) {
    statusService.getConnectorStatus().validateConnectorStatusIn(statuses);
  }

  @Override
  public void updateStatus(ConnectorStatus status) {
    statusService.updateConnectorStatus(new FullConnectorStatus(status, FINALIZED));
  }

  @Override
  public void validateRequiredPrivileges() {
    PrivilegeTools.validatePrivileges(session, REQUIRED_PRIVILEGES);
  }

  @Override
  public ConnectorResponse withRollbackHandling(Supplier<ConnectorResponse> action) {
    try {
      return checkResponse(action.get());
    } catch (ConnectorException exception) {
      return checkResponse(exception.getResponse());
    } catch (Exception exception) {
      return unknownErrorResponse();
    }
  }

  private ConnectorResponse checkResponse(ConnectorResponse response) {
    if (response.isOk()) {
      return response;
    }

    if (response.is(ROLLBACK_CODE)) {
      updateStatus(statusAfterRollback);
      return response;
    }

    return unknownErrorResponse();
  }

  private ConnectorResponse unknownErrorResponse() {
    updateStatus(ERROR);
    return ConnectorResponse.error(
        UNKNOWN_ERROR_CODE,
        "An unknown error occurred and the connector is now in an unspecified state. Contact "
            + "your connector provider, manual intervention may be required.");
  }
}
