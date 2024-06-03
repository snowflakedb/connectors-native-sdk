/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connection;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link ConnectionConfigurationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConnectionConfigurationInputValidator}
 *   <li>{@link ConnectionConfigurationCallback}
 *   <li>{@link ConnectionValidator}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectionConfigurationService}
 *   <li>{@link ConnectorStatusService}
 * </ul>
 */
public class ConnectionConfigurationHandlerTestBuilder {

  private ConnectionConfigurationInputValidator inputValidator;
  private ConnectionConfigurationCallback callback;
  private ConnectionValidator connectionValidator;
  private ConnectorErrorHelper errorHelper;
  private ConnectionConfigurationService connectionConfigurationService;
  private ConnectorStatusService connectorStatusService;

  /**
   * Creates a new, empty {@link ConnectionConfigurationHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public ConnectionConfigurationHandlerTestBuilder() {}

  /**
   * Creates a new {@link ConnectionConfigurationHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ConnectionConfigurationInputValidator}
   *   <li>a default implementation of {@link ConnectionConfigurationCallback}
   *   <li>a default implementation of {@link ConnectionValidator}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectionConfigurationService}
   *   <li>a default implementation of {@link ConnectorStatusService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  ConnectionConfigurationHandlerTestBuilder(Session session) {
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
  public ConnectionConfigurationHandlerTestBuilder withInputValidator(
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
  public ConnectionConfigurationHandlerTestBuilder withCallback(
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
  public ConnectionConfigurationHandlerTestBuilder withConnectionValidator(
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
  public ConnectionConfigurationHandlerTestBuilder withErrorHelper(
      ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the connection configuration service used to build the handler instance.
   *
   * @param connectionConfigurationService connection configuration service
   * @return this builder
   */
  public ConnectionConfigurationHandlerTestBuilder withConnectionConfigurationService(
      ConnectionConfigurationService connectionConfigurationService) {
    this.connectionConfigurationService = connectionConfigurationService;
    return this;
  }

  /**
   * Sets the connector status service used to build the handler instance.
   *
   * @param connectorStatusService connector status service
   * @return this builder
   */
  public ConnectionConfigurationHandlerTestBuilder withConnectorStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
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
