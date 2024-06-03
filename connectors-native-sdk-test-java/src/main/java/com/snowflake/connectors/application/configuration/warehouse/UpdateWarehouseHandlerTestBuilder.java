/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Test builder for the {@link UpdateWarehouseHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link UpdateWarehouseInputValidator}
 *   <li>{@link UpdateWarehouseCallback}
 *   <li>{@link UpdateWarehouseSdkCallback}
 *   <li>{@link ConnectorErrorHelper}
 *   <li>{@link ConnectorStatusService}
 *   <li>{@link ConnectorConfigurationService}
 * </ul>
 */
public class UpdateWarehouseHandlerTestBuilder extends UpdateWarehouseHandlerBuilder {

  /**
   * Creates a new {@link UpdateWarehouseHandlerTestBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link UpdateWarehouseInputValidator}
   *   <li>a default implementation of {@link UpdateWarehouseCallback}
   *   <li>a default implementation of {@link UpdateWarehouseSdkCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   *   <li>a default implementation of {@link ConnectorStatusService}
   *   <li>a default implementation of {@link ConnectorConfigurationService}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public UpdateWarehouseHandlerTestBuilder(Session session) {
    super(session);
  }

  /**
   * Creates a new, empty {@link UpdateWarehouseHandlerTestBuilder}.
   *
   * <p>Properties of the new builder instance must be fully customized before a handler instance
   * can be built.
   */
  public UpdateWarehouseHandlerTestBuilder() {
    super();
  }

  /** {@inheritDoc} */
  public UpdateWarehouseHandlerTestBuilder withInputValidator(
      UpdateWarehouseInputValidator inputValidator) {
    super.withInputValidator(inputValidator);
    return this;
  }

  /** {@inheritDoc} */
  public UpdateWarehouseHandlerTestBuilder withCallback(UpdateWarehouseCallback callback) {
    super.withCallback(callback);
    return this;
  }

  /**
   * Sets the sdk callback used to build the handler instance.
   *
   * @param sdkCallback warehouse update sdk callback
   * @return this builder
   */
  public UpdateWarehouseHandlerTestBuilder withSdkCallback(UpdateWarehouseSdkCallback sdkCallback) {
    this.sdkCallback = sdkCallback;
    return this;
  }

  /** {@inheritDoc} */
  public UpdateWarehouseHandlerTestBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    super.withErrorHelper(errorHelper);
    return this;
  }

  /**
   * Sets the connector status service used to build the handler instance.
   *
   * @param connectorStatusService connector status service
   * @return this builder
   */
  public UpdateWarehouseHandlerTestBuilder withConnectorStatusService(
      ConnectorStatusService connectorStatusService) {
    this.connectorStatusService = connectorStatusService;
    return this;
  }

  /**
   * Sets the connector configuration service used to build the handler instance.
   *
   * @param connectorConfigurationService connector configuration service
   * @return this builder
   */
  public UpdateWarehouseHandlerTestBuilder withConnectorConfigurationService(
      ConnectorConfigurationService connectorConfigurationService) {
    this.connectorConfigurationService = connectorConfigurationService;
    return this;
  }
}
