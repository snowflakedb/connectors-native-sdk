/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.common.table.DefaultKeyValueTable;
import com.snowflake.connectors.common.table.KeyNotFoundException;
import com.snowflake.connectors.util.variant.VariantMapperException;
import com.snowflake.snowpark_java.Session;

/** Service for basic management of the connector and connector configuration statuses. */
public interface ConnectorStatusService {

  /**
   * Returns the connector and connector configuration statuses.
   *
   * @return connector and connector configuration statuses
   * @throws KeyNotFoundException if the status record is not found in the state table
   * @throws VariantMapperException if the status record value is not a valid status Variant
   */
  FullConnectorStatus getConnectorStatus();

  /**
   * Updates the connector and connector configuration statuses.
   *
   * @param connectorStatus new statuses
   */
  void updateConnectorStatus(FullConnectorStatus connectorStatus);

  /**
   * Returns a new instance of the default service implementation.
   *
   * <p>Default implementation of the service uses:
   *
   * <ul>
   *   <li>a default implementation of {@link
   *       com.snowflake.connectors.common.state.KeyValueStateRepository KeyValueStateRepository},
   *       created for the {@code STATE.APP_STATE} table.
   *   <li>{@link ConnectorStatusRepository#CONNECTOR_STATUS_KEY CONNECTOR_STATUS_KEY} as the status
   *       record key
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new service instance
   */
  static ConnectorStatusService getInstance(Session session) {
    var table = new DefaultKeyValueTable(session, "STATE.APP_STATE");
    var statusRepository = ConnectorStatusRepository.getInstance(table);
    return new DefaultConnectorStatusService(statusRepository);
  }
}
