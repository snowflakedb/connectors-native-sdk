/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.common.state.KeyValueStateRepository;

/** Default implementation of {@link ConnectorStatusRepository}. */
class DefaultConnectorStatusRepository implements ConnectorStatusRepository {

  private final KeyValueStateRepository<FullConnectorStatus> stateRepository;

  DefaultConnectorStatusRepository(KeyValueStateRepository<FullConnectorStatus> stateRepository) {
    this.stateRepository = stateRepository;
  }

  @Override
  public FullConnectorStatus fetchConnectorStatus() {
    return stateRepository.fetch(CONNECTOR_STATUS_KEY);
  }

  @Override
  public void updateConnectorStatus(FullConnectorStatus fullConnectorStatus) {
    stateRepository.update(CONNECTOR_STATUS_KEY, fullConnectorStatus);
  }
}
