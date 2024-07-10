/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSING;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the connector pausing process. A new instance of the handler must be created using
 * {@link #builder(Session) the builder}.
 */
public class PauseConnectorHandler {

  /** Error type for the connector pausing failure, used by the {@link ConnectorErrorHelper}. */
  public static final String ERROR_TYPE = "PAUSE_CONNECTOR_FAILED";

  private final PauseConnectorStateValidator stateValidator;
  private final PauseConnectorCallback callback;
  private final ConnectorErrorHelper errorHelper;
  private final LifecycleService lifecycleService;
  private final PauseConnectorSdkCallback sdkCallback;
  private final PauseTaskReactorService pauseTaskReactorService;

  PauseConnectorHandler(
      PauseConnectorStateValidator stateValidator,
      PauseConnectorCallback callback,
      ConnectorErrorHelper errorHelper,
      LifecycleService lifecycleService,
      PauseConnectorSdkCallback sdkCallback,
      PauseTaskReactorService pauseTaskReactorService) {
    this.stateValidator = stateValidator;
    this.callback = callback;
    this.errorHelper = errorHelper;
    this.lifecycleService = lifecycleService;
    this.sdkCallback = sdkCallback;
    this.pauseTaskReactorService = pauseTaskReactorService;
  }

  /**
   * Default handler method for the {@code PUBLIC.PAUSE_CONNECTOR} procedure.
   *
   * @param session Snowpark session object
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #pauseConnector() pauseConnector}
   */
  public static Variant pauseConnector(Session session) {
    var handler = PauseConnectorHandler.builder(session).build();
    return handler.pauseConnector().toVariant();
  }

  /**
   * Returns a new instance of {@link PauseConnectorHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static PauseConnectorHandlerBuilder builder(Session session) {
    return new PauseConnectorHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector privileges check
   *   <li>connector status check
   *   <li>{@link PauseConnectorStateValidator#validate()}
   *   <li>{@link PauseConnectorCallback#execute()}
   *   <li>{@link PauseConnectorSdkCallback#execute()}
   *   <li>{@link PauseTaskReactorService#pauseAllInstances()}
   *   <li>connector status update
   * </ul>
   *
   * <p>If the callback execution returns a {@link LifecycleService#ROLLBACK_CODE ROLLBACK} code,
   * any changes made by this handler are also rolled back.
   *
   * @return a response with the code {@code OK} if the execution was successful, a response with
   *     the code {@link LifecycleService#ROLLBACK_CODE ROLLBACK} if a rollback operation was
   *     performed, or a response with an error code and an error message
   */
  public ConnectorResponse pauseConnector() {
    return errorHelper.withExceptionLoggingAndWrapping(this::pauseConnectorBody);
  }

  private ConnectorResponse pauseConnectorBody() {
    lifecycleService.validateRequiredPrivileges();
    lifecycleService.validateStatus(STARTED, PAUSING);

    var validationResult = stateValidator.validate();
    if (validationResult.isNotOk()) {
      return validationResult;
    }

    lifecycleService.updateStatus(PAUSING);

    ConnectorResponse internalResult = lifecycleService.withRollbackHandling(callback::execute);
    if (internalResult.isNotOk()) {
      return internalResult;
    }

    ConnectorResponse callbackResult = lifecycleService.withRollbackHandling(sdkCallback::execute);
    if (callbackResult.isNotOk()) {
      return callbackResult;
    }

    pauseTaskReactorService.pauseAllInstances();

    lifecycleService.updateStatus(PAUSED);
    return ConnectorResponse.success("Connector successfully paused.");
  }
}
