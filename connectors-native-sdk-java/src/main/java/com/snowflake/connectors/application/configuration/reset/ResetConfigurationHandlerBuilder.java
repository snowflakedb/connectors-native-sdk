/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link ResetConfigurationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ResetConfigurationValidator}
 *   <li>{@link ResetConfigurationCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class ResetConfigurationHandlerBuilder {

  /** See {@link ResetConfigurationValidator}. */
  protected ResetConfigurationValidator validator;

  /** See {@link ResetConfigurationSdkCallback}. */
  protected ResetConfigurationSdkCallback sdkCallback;

  /** See {@link ResetConfigurationCallback}. */
  protected ResetConfigurationCallback callback;

  /** See {@link ConnectorErrorHelper}. */
  protected ConnectorErrorHelper errorHelper;

  /** See {@link ConnectorStatusService}. */
  protected ConnectorStatusService connectorStatusService;

  /**
   * Creates a new {@link ResetConfigurationHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ResetConfigurationValidator}
   *   <li>a default implementation of {@link ResetConfigurationCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public ResetConfigurationHandlerBuilder(Session session) {
    requireNonNull(session);

    this.validator = new DefaultResetConfigurationValidator(session);
    this.sdkCallback = new DefaultResetConfigurationSdkCallback(session);
    this.callback = new InternalResetConfigurationCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ResetConfigurationHandler.ERROR_TYPE);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  ResetConfigurationHandlerBuilder() {}

  /**
   * Sets the validator used to build the handler instance.
   *
   * @param validator configuration reset validator
   * @return this builder
   */
  public ResetConfigurationHandlerBuilder withValidator(ResetConfigurationValidator validator) {
    this.validator = validator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback configuration reset callback
   * @return this builder
   */
  public ResetConfigurationHandlerBuilder withCallback(ResetConfigurationCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ResetConfigurationHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ResetConfigurationHandler build() {
    requireNonNull(validator);
    requireNonNull(sdkCallback);
    requireNonNull(callback);
    requireNonNull(errorHelper);
    requireNonNull(connectorStatusService);

    return new ResetConfigurationHandler(
        validator, sdkCallback, callback, errorHelper, connectorStatusService);
  }
}
