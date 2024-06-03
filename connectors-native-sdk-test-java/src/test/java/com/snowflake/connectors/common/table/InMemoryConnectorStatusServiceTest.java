/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.application.status.ConnectorStatusRepository;
import com.snowflake.connectors.application.status.ConnectorStatusService;
import com.snowflake.connectors.application.status.DefaultConnectorStatusService;
import com.snowflake.connectors.application.status.FullConnectorStatus;
import com.snowflake.connectors.application.status.exception.ConnectorStatusNotFoundException;
import com.snowflake.connectors.common.exception.InternalConnectorException;
import com.snowflake.snowpark_java.types.Variant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryConnectorStatusServiceTest {

  KeyValueTable table;
  ConnectorStatusRepository connectorStatusRepository;
  ConnectorStatusService connectorStatusService;

  @BeforeEach
  void setupSpec() {
    table = new InMemoryDefaultKeyValueTable();
    connectorStatusRepository = ConnectorStatusRepository.getInstance(table);
    connectorStatusService = new DefaultConnectorStatusService(connectorStatusRepository);
  }

  @Test
  void shouldInsertAndUpdateConnectorState() {
    // given
    var stateToInsert = new FullConnectorStatus(CONFIGURING, INSTALLED);
    var stateToUpdate = new FullConnectorStatus(PAUSED, null);

    // when
    connectorStatusService.updateConnectorStatus(stateToInsert);

    // then
    var insertedStatus = connectorStatusService.getConnectorStatus();
    assertThat(insertedStatus).isEqualTo(stateToInsert);

    // when
    connectorStatusService.updateConnectorStatus(stateToUpdate);

    // then
    var updatedStatus = connectorStatusService.getConnectorStatus();
    assertThat(updatedStatus).isEqualTo(stateToUpdate);
  }

  @Test
  void shouldThrowExceptionWhenConnectorStatusRecordDoesNotExistInDb() {
    // given
    table.delete("connector_status");

    // then
    assertThatExceptionOfType(ConnectorStatusNotFoundException.class)
        .isThrownBy(() -> connectorStatusService.getConnectorStatus());
  }

  @Test
  void shouldThrowExceptionWhenConnectorStatusCannotBeParsedToFullConnectorStatus() {
    // given
    table.delete("connector_status");
    table.update("connector_status", new Variant("123"));

    // then
    assertThatExceptionOfType(InternalConnectorException.class)
        .isThrownBy(() -> connectorStatusService.getConnectorStatus());
  }
}
