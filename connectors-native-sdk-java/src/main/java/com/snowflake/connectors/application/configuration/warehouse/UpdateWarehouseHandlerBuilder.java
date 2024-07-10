/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.snowpark_java.Session;

/**
 * Builder for the {@link UpdateWarehouseHandler}.
 *
 * <p>Allows for customization of the following handler components:
 *
 * <ul>
 *   <li>{@link UpdateWarehouseInputValidator}
 *   <li>{@link UpdateWarehouseCallback}
 *   <li>{@link ConnectorErrorHelper}
 * </ul>
 */
public class UpdateWarehouseHandlerBuilder {

  protected UpdateWarehouseInputValidator inputValidator;
  protected UpdateWarehouseCallback callback;
  protected UpdateWarehouseSdkCallback sdkCallback;
  protected ConnectorErrorHelper errorHelper;
  protected ConnectorStatusService connectorStatusService;
  protected ConnectorConfigurationService connectorConfigurationService;

  /**
   * Creates a new {@link UpdateWarehouseHandlerBuilder}.
   *
   * <p>The properties of this builder are initialised with:
   *
   * <ul>
   *   <li>a default implementation of {@link UpdateWarehouseInputValidator}
   *   <li>a default implementation of {@link UpdateWarehouseCallback}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @throws NullPointerException if the provided session object is null
   */
  public UpdateWarehouseHandlerBuilder(Session session) {
    requireNonNull(session);

    this.inputValidator = UpdateWarehouseInputValidator.getInstance(session);
    this.callback = new InternalUpdateWarehouseCallback(session);
    this.sdkCallback = new DefaultUpdateWarehouseSdkCallback(session);
    this.errorHelper =
        ConnectorErrorHelper.buildDefault(session, UpdateWarehouseHandler.ERROR_TYPE);
    this.connectorStatusService = ConnectorStatusService.getInstance(session);
    this.connectorConfigurationService = ConnectorConfigurationService.getInstance(session);
  }

  /** Constructor used by the test builder implementation. */
  UpdateWarehouseHandlerBuilder() {}

  /**
   * Sets the input validator used to build the handler instance.
   *
   * @param inputValidator warehouse update input validator
   * @return this builder
   */
  public UpdateWarehouseHandlerBuilder withInputValidator(
      UpdateWarehouseInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    return this;
  }

  /**
   * Sets the callback used to build the handler instance.
   *
   * @param callback warehouse update callback
   * @return this builder
   */
  public UpdateWarehouseHandlerBuilder withCallback(UpdateWarehouseCallback callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Sets the error helper used to build the handler instance.
   *
   * @param errorHelper connector error helper
   * @return this builder
   */
  public UpdateWarehouseHandlerBuilder withErrorHelper(ConnectorErrorHelper errorHelper) {
    this.errorHelper = errorHelper;
    return this;
  }

  /**
   * Builds a new handler instance.
   *
   * @return new handler instance
   * @throws NullPointerException if any property for the new handler is null
   */
  public UpdateWarehouseHandler build() {
    requireNonNull(inputValidator);
    requireNonNull(callback);
    requireNonNull(sdkCallback);
    requireNonNull(errorHelper);
    requireNonNull(connectorStatusService);
    requireNonNull(connectorConfigurationService);

    return new UpdateWarehouseHandler(
        inputValidator,
        callback,
        sdkCallback,
        errorHelper,
        connectorStatusService,
        connectorConfigurationService);
  }
}
