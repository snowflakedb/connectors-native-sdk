/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.resume;

import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.InstanceStreamService;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link ResumeConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ResumeConnectorStateValidator}
 *   <li>{@link ResumeConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class ResumeConnectorHandlerBuilder {

  private ResumeConnectorStateValidator stateValidator;
  private ResumeConnectorCallback callback;
  private ConnectorErrorHelper errorHelper;
  private final LifecycleService lifecycleService;
  private final ResumeConnectorSdkCallback resumeConnectorSdkCallback;
  private final InstanceStreamService instanceStreamService;
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor;
  private final ResumeTaskReactorService resumeTaskReactorService;

  /**
   * Creates a new {@link ResumeConnectorHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ResumeConnectorStateValidator}
   *   <li>a default implementation of {@link ResumeConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  ResumeConnectorHandlerBuilder(Session session) {
    requireNonNull(session);

    this.stateValidator = new DefaultResumeConnectorStateValidator(session);
    this.callback = new InternalResumeConnectorCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ResumeConnectorHandler.ERROR_TYPE);
    this.lifecycleService = LifecycleService.getInstance(session, PAUSED);
    this.resumeConnectorSdkCallback = DefaultResumeConnectorSdkCallback.getInstance(session);
    this.resumeTaskReactorService = ResumeTaskReactorService.getInstance(session);
    this.instanceStreamService = InstanceStreamService.getInstance(session);
    this.taskReactorInstanceActionExecutor = TaskReactorInstanceActionExecutor.getInstance(session);
  }

  /**
   * Sets the state validator used to build the handler instance.
   *
   * @param stateValidator connector state validator
   * @return this builder
   */
  public ResumeConnectorHandlerBuilder withStateValidator(
      ResumeConnectorStateValidator stateValidator) {
    this.stateValidator = stateValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback resume connector callback
   * @return this builder
   */
  public ResumeConnectorHandlerBuilder withCallback(ResumeConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ResumeConnectorHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ResumeConnectorHandler build() {
    requireNonNull(stateValidator);
    requireNonNull(callback);
    requireNonNull(errorHelper);
    requireNonNull(lifecycleService);

    return new ResumeConnectorHandler(
        stateValidator,
        callback,
        errorHelper,
        lifecycleService,
        resumeConnectorSdkCallback,
        instanceStreamService,
        taskReactorInstanceActionExecutor,
        resumeTaskReactorService);
  }
}
