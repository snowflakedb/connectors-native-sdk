/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.status;

import static com.snowflake.connectors.application.status.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.application.status.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.application.status.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.application.status.exception.ConnectorStatusNotFoundException;
import com.snowflake.connectors.common.exception.InternalConnectorException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConnectorStatusServiceTest extends BaseIntegrationTest {

  private ConnectorStatusService connectorStatusService;

  @BeforeAll
  void beforeAll() {
    connectorStatusService = ConnectorStatusService.getInstance(session);
  }

  @Test
  void shouldInsertAndUpdateConnectorState() {
    // given
    FullConnectorStatus stateToInsert = new FullConnectorStatus(CONFIGURING, INSTALLED);
    FullConnectorStatus stateToUpdate = new FullConnectorStatus(PAUSED, null);

    // when
    connectorStatusService.updateConnectorStatus(stateToInsert);

    // then
    FullConnectorStatus insertedStatus = connectorStatusService.getConnectorStatus();
    assertThat(insertedStatus).isInStatus(stateToInsert.getStatus());

    // when
    connectorStatusService.updateConnectorStatus(stateToUpdate);

    // then
    FullConnectorStatus updatedStatus = connectorStatusService.getConnectorStatus();
    assertThat(updatedStatus).isInStatus(stateToUpdate.getStatus());
  }

  @Test
  void shouldThrowExceptionWhenConnectorStatusRecordDoesNotExistInDb() {
    // given
    session.sql("DELETE FROM STATE.APP_STATE WHERE KEY = 'connector_status'").collect();

    // expect
    assertThatThrownBy(() -> connectorStatusService.getConnectorStatus())
        .isInstanceOf(ConnectorStatusNotFoundException.class)
        .hasMessage("Connector status record does not exist in database.");
  }

  @Test
  void shouldThrowExceptionWhenConnectorStatusCannotBeParsedToFullConnectorStatus() {
    // given
    session.sql("DELETE FROM STATE.APP_STATE WHERE KEY = 'connector_status'").collect();
    session
        .sql(
            "INSERT INTO STATE.APP_STATE (KEY, VALUE, UPDATED_AT) SELECT 'connector_status',"
                + " PARSE_JSON('123'), SYSDATE()")
        .collect();

    // expect
    assertThatThrownBy(() -> connectorStatusService.getConnectorStatus())
        .isInstanceOf(InternalConnectorException.class)
        .hasMessage("Cannot parse json value fetched from database to connector status object");
  }
}
