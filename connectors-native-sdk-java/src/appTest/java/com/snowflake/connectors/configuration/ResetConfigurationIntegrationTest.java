/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static com.snowflake.connectors.util.RowUtil.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.util.sql.SqlTools;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ResetConfigurationIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldResetConfiguration() {
    // given
    insertCompletedPrerequisites();
    configureConnector();
    setConnectionConfiguration();

    // when
    var response = callProcedure("RESET_CONFIGURATION()");

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertPrerequisitesTableContent(row("1", false), row("2", false));
    assertConfigurationIsEmpty();
    assertInternalStatus(CONFIGURING, INSTALLED);
  }

  private void insertCompletedPrerequisites() {
    executeInApp("TRUNCATE TABLE IF EXISTS STATE.PREREQUISITES");
    executeInApp(
        "INSERT INTO STATE.PREREQUISITES (ID, TITLE, DESCRIPTION, POSITION, IS_COMPLETED) VALUES "
            + " ('1', 'example', 'example', 2, true),"
            + " ('2', 'example', 'example', 1, true) ");
  }

  private void configureConnector() {
    var connectorConfig =
        new Variant(
            Map.of(
                "warehouse", "xsmall",
                "destination_database", "dest_db",
                "destination_schema", "dest_schema",
                "data_owner_role", "role"));
    callProcedure(format("CONFIGURE_CONNECTOR(%s)", SqlTools.asVariant(connectorConfig)));
    assertConnectorConfigSaved(connectorConfig);
  }

  private void setConnectionConfiguration() {
    var connectionConfig =
        new Variant(
            Map.of(
                "k1", "v1",
                "k2", "v2"));
    mockProcedure("TEST_CONNECTION()", "OK", "Connection test successful");
    callProcedure(format("SET_CONNECTION_CONFIGURATION(%s)", SqlTools.asVariant(connectionConfig)));
    assertConnectionConfigSaved(connectionConfig);
  }

  private void assertConnectorConfigSaved(Variant expectedConfig) {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    var savedConfig = executeInApp(query)[0].getVariant(0);
    assertThat(savedConfig).isEqualTo(expectedConfig);
  }

  private void assertConnectionConfigSaved(Variant expectedConfig) {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connection_configuration'";
    var savedConfig = executeInApp(query)[0].getVariant(0);
    assertThat(savedConfig).isEqualTo(expectedConfig);
  }

  private void assertPrerequisitesTableContent(Row... expectedContent) {
    var result = executeInApp("SELECT id, is_completed FROM STATE.PREREQUISITES");
    assertThat(result).containsExactlyInAnyOrder(expectedContent);
  }

  private void assertConfigurationIsEmpty() {
    var result = executeInApp("SELECT * FROM STATE.APP_CONFIG");
    assertThat(result.length).isEqualTo(0);
  }
}
