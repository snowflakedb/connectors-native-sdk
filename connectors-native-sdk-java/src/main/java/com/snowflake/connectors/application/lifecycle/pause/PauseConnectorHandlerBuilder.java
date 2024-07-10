/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.lifecycle.pause;

import static com.snowflake.connectors.application.status.ConnectorStatus.STARTED;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.lifecycle.LifecycleService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link PauseConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link PauseConnectorStateValidator}
 *   <li>{@link PauseConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class PauseConnectorHandlerBuilder {

  private PauseConnectorStateValidator stateValidator;
  private PauseConnectorCallback callback;
  private ConnectorErrorHelper errorHelper;
  private final LifecycleService lifecycleService;
  private final PauseConnectorSdkCallback sdkCallback;
  private final PauseTaskReactorService pauseTaskReactorService;

  /**
   * Creates a new {@link PauseConnectorHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link PauseConnectorStateValidator}
   *   <li>a default implementation of {@link PauseConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public PauseConnectorHandlerBuilder(Session session) {
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
  public PauseConnectorHandlerBuilder withStateValidator(
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
  public PauseConnectorHandlerBuilder withCallback(PauseConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public PauseConnectorHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
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
