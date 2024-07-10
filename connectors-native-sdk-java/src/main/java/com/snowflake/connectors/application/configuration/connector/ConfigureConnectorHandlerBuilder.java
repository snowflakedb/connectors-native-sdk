/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link ConfigureConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConfigureConnectorInputValidator}
 *   <li>{@link ConfigureConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class ConfigureConnectorHandlerBuilder {

  ConfigureConnectorInputValidator inputValidator;
  ConfigureConnectorCallback callback;
  ConnectorErrorHelper errorHelper;
  ConnectorConfigurationService connectorConfigurationService;
  ConnectorStatusService connectorStatusService;

  /** Constructor used by the test builder implementation. */
  ConfigureConnectorHandlerBuilder() {}

  /**
   * Creates a new {@link ConfigureConnectorHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ConfigureConnectorInputValidator}
   *   <li>a default implementation of {@link ConfigureConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public ConfigureConnectorHandlerBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = new DefaultConfigureConnectorInputValidator(session);
    this.callback = new InternalConfigureConnectorCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, ConfigureConnectorHandler.ERROR_TYPE);
    this.connectorConfigurationService = ConnectorConfigurationService.getInstance(session);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator connector configuration input validator
   * @return this builder
   */
  public ConfigureConnectorHandlerBuilder withInputValidator(
      ConfigureConnectorInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback connector configuration callback
   * @return this builder
   */
  public ConfigureConnectorHandlerBuilder withCallback(ConfigureConnectorCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ConfigureConnectorHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public ConfigureConnectorHandler build() {
    requireNonNull(inputValidator);
    requireNonNull(callback);
    requireNonNull(errorHelper);
    requireNonNull(connectorConfigurationService);
    requireNonNull(connectorStatusService);

    return new ConfigureConnectorHandler(
        inputValidator,
        callback,
        errorHelper,
        connectorConfigurationService,
        connectorStatusService);
  }
}
