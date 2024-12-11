/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.reset;

import com.snowflake.connectors.application.configuration.ConfigurationRepository;
import com.snowflake.connectors.application.configuration.prerequisites.PrerequisitesRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link ResetConfigurationHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link ResetConfigurationValidator}
 *   <li>{@link ResetConfigurationSdkCallback}
 *   <li>{@link ResetConfigurationCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectorStatusService}
 *   <li>{@link ConfigurationRepository}
 *   <li>{@link PrerequisitesRepository}
 *   <li>{@link TransactionManager}
 * </ul>
 */
public class ResetConfigurationHandlerTestBuilder extends ResetConfigurationHandlerBuilder {

  /**
   * Creates a new, empty {@link ResetConfigurationHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public ResetConfigurationHandlerTestBuilder() {
    super();
  }

  /**
   * Creates a new {@link ResetConfigurationHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link ResetConfigurationValidator}
   *   <li>a default implementation of {@link ResetConfigurationSdkCallback}
   *   <li>a default implementation of {@link ResetConfigurationCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectorStatusService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public ResetConfigurationHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Sets the validator used to build the handler instance.
   *
   * @param validator connector state validator
   * @return this builder
   */
  public ResetConfigurationHandlerTestBuilder withValidator(ResetConfigurationValidator validator) {
    this.validator = validator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param sdkCallback pre reset configuration callback
   * @return this builder
   */
  public ResetConfigurationHandlerTestBuilder withSdkCallback(
      ResetConfigurationSdkCallback sdkCallback) {
    this.sdkCallback = sdkCallback;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback reset configuration callback
   * @return this builder
   */
  public ResetConfigurationHandlerTestBuilder withCallback(ResetConfigurationCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public ResetConfigurationHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Sets the connector status service used to build the handler instance.
   *
   * @param connectorStatusService connector status service
   * @return this builder
   */
  public ResetConfigurationHandlerTestBuilder withConnectorStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
    return this;
  }
}
