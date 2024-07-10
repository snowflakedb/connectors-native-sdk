/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.common.state.KeyValueStateRepository;

/** InMemory implementation of {@link ConnectorStatusRepository}. */
public class InMemoryConnectorStatusRepository extends DefaultConnectorStatusRepository {

  public InMemoryConnectorStatusRepository(
      KeyValueStateRepository<FullConnectorStatus> stateRepository) {
    super(stateRepository);
  }
}
