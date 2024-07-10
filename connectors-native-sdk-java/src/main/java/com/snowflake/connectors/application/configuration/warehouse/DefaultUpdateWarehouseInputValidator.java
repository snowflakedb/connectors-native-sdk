/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.WAREHOUSE;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.AccessTools;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Optional;

/** Default implementation of {@link UpdateWarehouseInputValidator}. */
class DefaultUpdateWarehouseInputValidator implements UpdateWarehouseInputValidator {

  private final AccessTools accessTools;
  private final ConnectorConfigurationService connectorConfiguration;

  DefaultUpdateWarehouseInputValidator(
      AccessTools accessTools, ConnectorConfigurationService connectorConfiguration) {
    this.accessTools = accessTools;
    this.connectorConfiguration = connectorConfiguration;
  }

  @Override
  public ConnectorResponse validate(Identifier warehouse) {
    var currentWarehouse = getCurrentWarehouse();
    if (warehouse.equals(currentWarehouse)) {
      throw new WarehouseAlreadyUsedException(warehouse);
    }

    if (!accessTools.hasWarehouseAccess(warehouse)) {
      throw new InaccessibleWarehouseException(warehouse);
    }

    return ConnectorResponse.success();
  }

  private Identifier getCurrentWarehouse() {
    return Optional.of(connectorConfiguration.getConfiguration())
        .map(Variant::asMap)
        .flatMap(config -> Optional.ofNullable(config.get(WAREHOUSE.getPropertyName())))
        .map(Variant::asString)
        .map(Identifier::from)
        .orElse(null);
  }
}
