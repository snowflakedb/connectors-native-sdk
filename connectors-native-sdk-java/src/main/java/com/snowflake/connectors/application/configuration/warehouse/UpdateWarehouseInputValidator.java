/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.snowflake.AccessTools;
import com.snowflake.snowpark_java.Session;

/**
 * A validator for the input of {@link UpdateWarehouseHandler}, may be used to provide custom
 * warehouse validation.
 *
 * <p>Default implementation of this validator:
 *
 * <ul>
 *   <li>validates if a provided warehouse name is a valid identifier
 *   <li>validates if a provided warehouse name is different than the currently configured one
 *   <li>validates if the application can access the new warehouse
 * </ul>
 */
@FunctionalInterface
public interface UpdateWarehouseInputValidator {

  /**
   * Validates the provided warehouse.
   *
   * @param warehouse new warehouse name provided to the handler
   * @return a response with the code {@code OK} if the validation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse validate(Identifier warehouse);

  /**
   * Returns a new instance of {@link UpdateWarehouseInputValidator}.
   *
   * @param session Snowpark session object
   * @return a new Update Warehouse input validator.
   */
  static UpdateWarehouseInputValidator getInstance(Session session) {
    var accessTools = AccessTools.getInstance(session);
    var connectorConfiguration = ConnectorConfigurationService.getInstance(session);
    return new DefaultUpdateWarehouseInputValidator(accessTools, connectorConfiguration);
  }
}
