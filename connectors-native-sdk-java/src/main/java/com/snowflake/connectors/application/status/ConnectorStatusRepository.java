/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.common.state.DefaultKeyValueStateRepository;
import com.snowflake.connectors.common.table.KeyValueTable;

/** Repository for basic storage of the connector and connector configuration statuses. */
public interface ConnectorStatusRepository {

  /** Key of status record. */
  String CONNECTOR_STATUS_KEY = "connector_status";

  /**
   * Fetches the connector and connector configuration statuses.
   *
   * @return connector and connector configuration statuses
   */
  FullConnectorStatus fetchConnectorStatus();

  /**
   * Updates the connector and connector configuration statuses.
   *
   * @param fullConnectorStatus new statuses
   */
  void updateConnectorStatus(FullConnectorStatus fullConnectorStatus);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses:
   *
   * <ul>
   *   <li>a default implementation of {@link
   *       com.snowflake.connectors.common.state.KeyValueStateRepository KeyValueStateRepository}
   *   <li>{@link #CONNECTOR_STATUS_KEY} as the status record key
   * </ul>
   *
   * @param table table in which the status will be stored
   * @return a new repository instance
   */
  static ConnectorStatusRepository getInstance(KeyValueTable table) {
    var stateRepository = new DefaultKeyValueStateRepository<>(table, FullConnectorStatus.class);
    return new DefaultConnectorStatusRepository(stateRepository);
  }
}
