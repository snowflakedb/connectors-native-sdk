/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link ConnectionConfigurationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectionConfigurationInputValidator}
 *   <li>{@link ConnectionConfigurationCallback}
 *   <li>{@link ConnectionValidator}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class ConnectionConfigurationHandlerBuilder {

  private ConnectionConfigurationInputValidator inputValidator;
  private ConnectionConfigurationCallback callback;
  private ConnectionValidator connectionValidator;
  private ConnectorErrorHelper errorHelper;
  private final ConnectionConfigurationService connectionConfigurationService;
  private final ConnectorStatusService connectorStatusService;

  /**
   * Creates a new {@link ConnectionConfigurationHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ConnectionConfigurationInputValidator}
   *   <li>a default implementation of {@link ConnectionConfigurationCallback}
   *   <li>a default implementation of {@link ConnectionValidator}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  ConnectionConfigurationHandlerBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = new DefaultConnectionConfigurationInputValidator(session);
    this.callback = new InternalConnectionConfigurationCallback(session);
    this.connectionValidator = new TestConnectionValidator(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ConnectionConfigurationHandler.ERROR_TYPE);
    this.connectionConfigurationService = ConnectionConfigurationService.getInstance(session);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator connection configuration input validator
   * @return this builder
   */
  public ConnectionConfigurationHandlerBuilder withInputValidator(
      ConnectionConfigurationInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback connector configuration callback
   * @return this builder
   */
  public ConnectionConfigurationHandlerBuilder withCallback(
      ConnectionConfigurationCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the connection validator used to build the handler instance.
   *
   * @param connectionValidator connection validator
   * @return this builder
   */
  public ConnectionConfigurationHandlerBuilder withConnectionValidator(
      ConnectionValidator connectionValidator) {
    this.connectionValidator = connectionValidator;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ConnectionConfigurationHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ConnectionConfigurationHandler build() {
    requireNonNull(inputValidator);
    requireNonNull(callback);
    requireNonNull(connectionValidator);
    requireNonNull(errorHelper);
    requireNonNull(connectionConfigurationService);
    requireNonNull(connectorStatusService);

    return new ConnectionConfigurationHandler(
        inputValidator,
        callback,
        connectionValidator,
        errorHelper,
        connectionConfigurationService,
        connectorStatusService);
  }
}
