/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import com.snowflake.connectors.application.status.exception.ConnectorStatusNotFoundException;
import com.snowflake.connectors.common.exception.InternalConnectorException;
import com.snowflake.connectors.common.table.KeyNotFoundException;
import com.snowflake.connectors.util.variant.VariantMapperException;

/** Default implementation of {@link ConnectorStatusService}. */
public class DefaultConnectorStatusService implements ConnectorStatusService {

  private final ConnectorStatusRepository connectorStatusRepository;

  /**
   * Creates a new {@link DefaultConnectorStatusService}, using the provided status repository.
   *
   * @param connectorStatusRepository connector status repository
   */
  public DefaultConnectorStatusService(ConnectorStatusRepository connectorStatusRepository) {
    this.connectorStatusRepository = connectorStatusRepository;
  }

  @Override
  public FullConnectorStatus getConnectorStatus() {
    try {
      return connectorStatusRepository.fetchConnectorStatus();
    } catch (KeyNotFoundException e) {
      throw new ConnectorStatusNotFoundException();
    } catch (VariantMapperException e) {
      throw new InternalConnectorException(
          "Cannot parse json value fetched from database to connector status object");
    }
  }

  @Override
  public void updateConnectorStatus(FullConnectorStatus connectorStatus) {
    connectorStatusRepository.updateConnectorStatus(connectorStatus);
  }
}
