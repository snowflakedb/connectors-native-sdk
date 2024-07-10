/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.connector;

import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link ConfigureConnectorHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ConfigureConnectorInputValidator}
 *   <li>{@link ConfigureConnectorCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectorConfigurationService}
 *   <li>{@link ConnectorStatusService}
 * </ul>
 */
public class ConfigureConnectorHandlerTestBuilder extends ConfigureConnectorHandlerBuilder {

  /**
   * Creates a new, empty {@link ConfigureConnectorHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public ConfigureConnectorHandlerTestBuilder() {}

  /**
   * Creates a new {@link ConfigureConnectorHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ConfigureConnectorInputValidator}
   *   <li>a default implementation of {@link ConfigureConnectorCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectorConfigurationService}
   *   <li>a default implementation of {@link ConnectorStatusService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public ConfigureConnectorHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator connector configuration input validator
   * @return this builder
   */
  public ConfigureConnectorHandlerTestBuilder withInputValidator(
      ConfigureConnectorInputValidator inputValidator) {
    super.withInputValidator(inputValidator);
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback connector configuration callback
   * @return this builder
   */
  public ConfigureConnectorHandlerTestBuilder withCallback(ConfigureConnectorCallback callback) {
    super.withCallback(callback);
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ConfigureConnectorHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.withErrorHelper(errorHelper);
    return this;
  }

  /**
   * Sets the connector configuration service used to build the handler instance.
   *
   * @param connectorConfigurationService connector configuration service
   * @return this builder
   */
  public ConfigureConnectorHandlerTestBuilder withConfigurationService(
      ConnectorConfigurationService connectorConfigurationService) {
    this.connectorConfigurationService = connectorConfigurationService;
    return this;
  }

  /**
   * Sets the connector status service used to build the handler instance.
   *
   * @param connectorStatusService connector status service
   * @return this builder
   */
  public ConfigureConnectorHandlerTestBuilder withStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
    return this;
  }
}
