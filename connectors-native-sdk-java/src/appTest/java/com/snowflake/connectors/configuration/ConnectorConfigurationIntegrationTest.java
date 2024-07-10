/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.util.RowUtil.row;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ConnectorConfigurationIntegrationTest extends BaseNativeSdkIntegrationTest {

  @Test
  void shouldFlattenDataFromConnectorConfigurationTable() {
    // given
    insertExampleConfigData();

    // when
    var result =
        session
            .sql(
                "SELECT CONFIG_GROUP, CONFIG_KEY, VALUE "
                    + "FROM PUBLIC.CONNECTOR_CONFIGURATION "
                    + "ORDER BY CONFIG_GROUP, CONFIG_KEY")
            .collect();

    // then
    assertThat(result)
        .containsExactly(
            row("connection_configuration", "secret_name", "secret_db.schema.the_secret"),
            row("connector_configuration", "key_with_null_value", null),
            row("connector_configuration", "warehouse", "wh"),
            row("custom_configuration", "journal_table", "j_table_name"),
            // TODO: should be changed back to null when UI handles it
            row("flat_config_key", "flat_config_key", "flat_config_value"));
  }

  private void insertExampleConfigData() {
    var timestamp = "'" + Timestamp.from(Instant.now()) + "'";

    executeInApp(
        "INSERT INTO STATE.APP_CONFIG "
            + "SELECT 'connection_configuration', "
            + "OBJECT_CONSTRUCT('secret_name', 'secret_db.schema.the_secret'), "
            + timestamp);

    executeInApp(
        "INSERT INTO STATE.APP_CONFIG "
            + "SELECT 'custom_configuration', "
            + "OBJECT_CONSTRUCT('journal_table', 'j_table_name'), "
            + timestamp);

    executeInApp(
        "INSERT INTO STATE.APP_CONFIG "
            + "SELECT 'connector_configuration', "
            + "OBJECT_CONSTRUCT_KEEP_NULL('warehouse', 'wh', 'key_with_null_value', null), "
            + timestamp);

    executeInApp(
        "INSERT INTO STATE.APP_CONFIG "
            + "SELECT 'flat_config_key', "
            + "'flat_config_value'::variant, "
            + timestamp);
  }
}
