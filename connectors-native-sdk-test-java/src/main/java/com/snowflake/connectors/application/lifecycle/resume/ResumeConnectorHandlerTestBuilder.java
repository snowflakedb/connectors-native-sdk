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
 *   <li>{@link LifecycleService}
 *   <li>{@link ResumeConnectorSdkCallback}
 *   <li>{@link ResumeTaskReactorService}
 *   <li>{@link InstanceStreamService}
 *   <li>{@link TaskReactorInstanceActionExecutor}
 * </ul>
 */
public class ResumeConnectorHandlerTestBuilder {

  private ResumeConnectorStateValidator stateValidator;
  private ResumeConnectorCallback callback;
  private ConnectorErrorHelper errorHelper;
  private LifecycleService lifecycleService;
  private ResumeConnectorSdkCallback sdkCallback;
  private ResumeTaskReactorService resumeTaskReactorService;
  private InstanceStreamService instanceStreamService;
  private TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor;

  /**
   * Creates a new, empty {@link ResumeConnectorHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public ResumeConnectorHandlerTestBuilder() {}

  /**
   * Creates a new {@link ResumeConnectorHandler}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ResumeConnectorStateValidator}
   *   <li>a default implementation of {@link ResumeConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link LifecycleService}, with {@link
   *       com.snowflake.connectors.application.status.ConnectorStatus#PAUSED PAUSED} state as
   *       post-rollback status
   *   <li>a default implementation of {@link ResumeConnectorSdkCallback}
   *   <li>a default implementation of {@link ResumeTaskReactorService}
   *   <li>a default implementation of {@link InstanceStreamService}
   *   <li>a default implementation of {@link TaskReactorInstanceActionExecutor}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public ResumeConnectorHandlerTestBuilder(Session session) {
    requireNonNull(session);

    this.stateValidator = new DefaultResumeConnectorStateValidator(session);
    this.callback = new InternalResumeConnectorCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ResumeConnectorHandler.ERROR_TYPE);
    this.lifecycleService = LifecycleService.getInstance(session, PAUSED);
    this.sdkCallback = DefaultResumeConnectorSdkCallback.getInstance(session);
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
  public ResumeConnectorHandlerTestBuilder withStateValidator(
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
  public ResumeConnectorHandlerTestBuilder withCallback(ResumeConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the lifecycle service used to build the handler instance.
   *
   * @param lifecycleService lifecycle service
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withLifecycleService(LifecycleService lifecycleService) {
    this.lifecycleService = lifecycleService;
    return this;
  }

  /**
   * Sets the internal sdk callback used to build the handler instance.
   *
   * @param sdkCallback internal sdk callback
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withSdkCallback(ResumeConnectorSdkCallback sdkCallback) {
    this.sdkCallback = sdkCallback;
    return this;
  }

  /**
   * Sets the service used to resume Task Reactor instances.
   *
   * @param resumeTaskReactorService service which resumes Task Reactor
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withResumeTaskReactorService(
      ResumeTaskReactorService resumeTaskReactorService) {
    this.resumeTaskReactorService = resumeTaskReactorService;
    return this;
  }

  /**
   * Sets the task reactor instance stream service used to build the handler instance.
   *
   * @param instanceStreamService task reactor instance stream service
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withInstanceStreamService(
      InstanceStreamService instanceStreamService) {
    this.instanceStreamService = instanceStreamService;
    return this;
  }

  /**
   * Sets the task reactor instance action executor used to build the handler instance.
   *
   * @param taskReactorInstanceActionExecutor task reactor instance action executor
   * @return this builder
   */
  public ResumeConnectorHandlerTestBuilder withTaskReactorInstanceActionExecutor(
      TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor) {
    this.taskReactorInstanceActionExecutor = taskReactorInstanceActionExecutor;
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
    requireNonNull(sdkCallback);
    requireNonNull(instanceStreamService);
    requireNonNull(taskReactorInstanceActionExecutor);
    requireNonNull(resumeTaskReactorService);

    return new ResumeConnectorHandler(
        stateValidator,
        callback,
        errorHelper,
        lifecycleService,
        sdkCallback,
        instanceStreamService,
        taskReactorInstanceActionExecutor,
        resumeTaskReactorService);
  }
}
