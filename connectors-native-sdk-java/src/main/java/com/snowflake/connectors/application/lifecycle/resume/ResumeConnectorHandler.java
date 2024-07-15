/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static com.snowflake.connectors.application.status.ConnectorStatus.STARTING;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.taskreactor.InstanceStreamService;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the connector resuming process. A new instance of the handler must be created using
 * {@link #builder(Session) the builder}.
 */
public class ResumeConnectorHandler {

  /** Error type for the connector resuming failure, used by the {@link ConnectorErrorHelper}. */
  public static final String ERROR_TYPE = "RESUME_CONNECTOR_FAILED";

  private final ResumeConnectorStateValidator stateValidator;
  private final ResumeConnectorCallback callback;
  private final ConnectorErrorHelper errorHelper;
  private final LifecycleService lifecycleService;
  private final ResumeConnectorSdkCallback sdkCallback;
  private final InstanceStreamService instanceStreamService;
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor;
  private final ResumeTaskReactorService resumeTaskReactorService;

  ResumeConnectorHandler(
      ResumeConnectorStateValidator stateValidator,
      ResumeConnectorCallback callback,
      ConnectorErrorHelper errorHelper,
      LifecycleService lifecycleService,
      ResumeConnectorSdkCallback sdkCallback,
      InstanceStreamService instanceStreamService,
      TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor,
      ResumeTaskReactorService resumeTaskReactorService) {
    this.stateValidator = stateValidator;
    this.callback = callback;
    this.errorHelper = errorHelper;
    this.lifecycleService = lifecycleService;
    this.sdkCallback = sdkCallback;
    this.instanceStreamService = instanceStreamService;
    this.taskReactorInstanceActionExecutor = taskReactorInstanceActionExecutor;
    this.resumeTaskReactorService = resumeTaskReactorService;
  }

  /**
   * Default handler method for the {@code PUBLIC.RESUME_CONNECTOR} procedure.
   *
   * @param session Snowpark session object
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #resumeConnector() resumeConnector}
   */
  public static Variant resumeConnector(Session session) {
    var handler = ResumeConnectorHandler.builder(session).build();
    return handler.resumeConnector().toVariant();
  }

  /**
   * Returns a new instance of {@link ResumeConnectorHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static ResumeConnectorHandlerBuilder builder(Session session) {
    return new ResumeConnectorHandlerBuilder(session);
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
   *   <li>{@link ResumeConnectorStateValidator#validate()}
   *   <li>{@link ResumeConnectorCallback#execute()}
   *   <li>{@link ResumeConnectorSdkCallback#execute()}
   *   <li>{@link InstanceStreamService#recreateStreams(Identifier)} executed for all Task Reactor
   *       instances
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
  public ConnectorResponse resumeConnector() {
    return errorHelper.withExceptionLoggingAndWrapping(this::resumeConnectorBody);
  }

  private ConnectorResponse resumeConnectorBody() {
    lifecycleService.validateRequiredPrivileges();
    lifecycleService.validateStatus(PAUSED, STARTING);

    var validationResult = stateValidator.validate();
    if (validationResult.isNotOk()) {
      return validationResult;
    }

    lifecycleService.updateStatus(STARTING);

    ConnectorResponse internalResult = lifecycleService.withRollbackHandling(callback::execute);
    if (internalResult.isNotOk()) {
      return internalResult;
    }

    ConnectorResponse callbackResult = lifecycleService.withRollbackHandling(sdkCallback::execute);
    if (callbackResult.isNotOk()) {
      return callbackResult;
    }

    resumeTaskReactorService.resumeAllInstances();
    taskReactorInstanceActionExecutor.applyToAllInitializedTaskReactorInstances(
        instanceStreamService::recreateStreams);

    lifecycleService.updateStatus(STARTED);
    return ConnectorResponse.success("Connector successfully resumed.");
  }
}
