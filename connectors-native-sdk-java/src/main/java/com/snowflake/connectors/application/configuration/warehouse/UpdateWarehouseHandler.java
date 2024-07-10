/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey;
import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.application.status.ConnectorStatus;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the warehouse update process. A new instance of the handler must be created using
 * {@link #builder(Session) the builder}.
 */
public class UpdateWarehouseHandler {

  /** Error type for the warehouse update failure, used by the {@link ConnectorErrorHelper}. */
  public static final String ERROR_TYPE = "UPDATE_WAREHOUSE_FAILED";

  private final UpdateWarehouseInputValidator inputValidator;
  private final UpdateWarehouseCallback callback;
  private final UpdateWarehouseSdkCallback sdkCallback;
  private final ConnectorErrorHelper errorHelper;
  private final ConnectorStatusService connectorStatusService;
  private final ConnectorConfigurationService connectorConfigurationService;

  UpdateWarehouseHandler(
      UpdateWarehouseInputValidator inputValidator,
      UpdateWarehouseCallback callback,
      UpdateWarehouseSdkCallback sdkCallback,
      ConnectorErrorHelper errorHelper,
      ConnectorStatusService connectorStatusService,
      ConnectorConfigurationService connectorConfigurationService) {
    this.inputValidator = inputValidator;
    this.callback = callback;
    this.sdkCallback = sdkCallback;
    this.errorHelper = errorHelper;
    this.connectorStatusService = connectorStatusService;
    this.connectorConfigurationService = connectorConfigurationService;
  }

  /**
   * Default handler method for the {@code PUBLIC.UPDATE_WAREHOUSE} procedure.
   *
   * @param session Snowpark session object
   * @param warehouseName new warehouse name
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #updateWarehouse(String) updateWarehouse}
   */
  public static Variant updateWarehouse(Session session, String warehouseName) {
    var updateWarehouseHandler = UpdateWarehouseHandler.builder(session).build();
    return updateWarehouseHandler.updateWarehouse(warehouseName).toVariant();
  }

  /**
   * Returns a new instance of {@link UpdateWarehouseHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static UpdateWarehouseHandlerBuilder builder(Session session) {
    return new UpdateWarehouseHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of:
   *
   * <ul>
   *   <li>connector status check
   *   <li>{@link UpdateWarehouseInputValidator#validate(Identifier)}
   *   <li>{@link UpdateWarehouseCallback#execute(Identifier)}
   *   <li>{@link UpdateWarehouseSdkCallback#execute(Identifier)}
   *   <li>connector configuration update
   * </ul>
   *
   * @param warehouseName new warehouse name
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse updateWarehouse(String warehouseName) {
    return errorHelper.withExceptionLogging(() -> updateWarehouseBody(warehouseName));
  }

  private ConnectorResponse updateWarehouseBody(String warehouseName) {
    validateConnectorStatus();
    var warehouse = Identifier.from(warehouseName);

    var validationResult = inputValidator.validate(warehouse);
    if (validationResult.isNotOk()) {
      return validationResult;
    }

    var internalResult = callback.execute(warehouse);
    if (internalResult.isNotOk()) {
      return internalResult;
    }

    var sdkResult = sdkCallback.execute(warehouse);
    if (sdkResult.isNotOk()) {
      return sdkResult;
    }

    saveWarehouse(warehouse);
    return ConnectorResponse.success();
  }

  private void validateConnectorStatus() {
    connectorStatusService.getConnectorStatus().validateConnectorStatusIn(ConnectorStatus.PAUSED);
  }

  private void saveWarehouse(Identifier warehouse) {
    var newConfiguration = connectorConfigurationService.getConfiguration().asMap();
    newConfiguration.put(
        ConnectorConfigurationKey.WAREHOUSE.getPropertyName(), warehouse.getVariantValue());

    connectorConfigurationService.updateConfiguration(new Variant(newConfiguration));
  }
}
