/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link PauseConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link PauseConnectorStateValidator}
 *   <li>{@link PauseConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link LifecycleService}
 *   <li>{@link PauseConnectorSdkCallback}
 *   <li>{@link PauseTaskReactorService}
 * </ul>
 */
public class PauseConnectorHandlerTestBuilder {

  private PauseConnectorStateValidator stateValidator;
  private PauseConnectorCallback callback;
  private ConnectorErrorHelper errorHelper;
  private LifecycleService lifecycleService;
  private PauseConnectorSdkCallback sdkCallback;
  private PauseTaskReactorService pauseTaskReactorService;

  /**
   * Creates a new, empty {@link PauseConnectorHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public PauseConnectorHandlerTestBuilder() {}

  /**
   * Creates a new {@link PauseConnectorHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link PauseConnectorStateValidator}
   *   <li>a default implementation of {@link PauseConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link LifecycleService}, with {@link
   *       com.snowflake.connectors.application.status.ConnectorStatus#STARTED STARTED} state as
   *       post-rollback status
   *   <li>a default implementation of {@link PauseConnectorSdkCallback}
   *   <li>a default implementation of {@link PauseTaskReactorService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public PauseConnectorHandlerTestBuilder(Session session) {
    requireNonNull(session);

    this.stateValidator = new DefaultPauseConnectorStateValidator(session);
    this.callback = new InternalPauseConnectorCallback(session);
    this.errorHelper = ConnectorErrorHelper.buildDefault(session, PauseConnectorHandler.ERROR_TYPE);
    this.lifecycleService = LifecycleService.getInstance(session, STARTED);
    this.sdkCallback = DefaultPauseConnectorSdkCallback.getInstance(session);
    this.pauseTaskReactorService = PauseTaskReactorService.getInstance(session);
  }

  /**
   * Sets the state validator used to build the handler instance.
   *
   * @param stateValidator connector state validator
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withStateValidator(
      PauseConnectorStateValidator stateValidator) {
    this.stateValidator = stateValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback pause connector callback
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withCallback(PauseConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the lifecycle service used to build the handler instance.
   *
   * @param lifecycleService lifecycle service
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withLifecycleService(LifecycleService lifecycleService) {
    this.lifecycleService = lifecycleService;
    return this;
  }

  /**
   * Sets the internal sdk callback used to build the handler instance.
   *
   * @param sdkCallback internal sdk callback
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withSdkCallback(PauseConnectorSdkCallback sdkCallback) {
    this.sdkCallback = sdkCallback;
    return this;
  }

  /**
   * Sets the service used to pause Task Reactor instances.
   *
   * @param pauseTaskReactorService service which pauses Task Reactor
   * @return this builder
   */
  public PauseConnectorHandlerTestBuilder withPauseTaskReactorService(
      PauseTaskReactorService pauseTaskReactorService) {
    this.pauseTaskReactorService = pauseTaskReactorService;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public PauseConnectorHandler build() {
    requireNonNull(stateValidator);
    requireNonNull(callback);
    requireNonNull(errorHelper);
    requireNonNull(lifecycleService);
    requireNonNull(sdkCallback);
    requireNonNull(pauseTaskReactorService);

    return new PauseConnectorHandler(
        stateValidator,
        callback,
        errorHelper,
        lifecycleService,
        sdkCallback,
        pauseTaskReactorService);
  }
}
