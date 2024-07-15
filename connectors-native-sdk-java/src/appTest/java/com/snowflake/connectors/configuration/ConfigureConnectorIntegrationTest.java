/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.INSTALLED;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.junit.jupiter.api.Test;

class ConfigureConnectorIntegrationTest extends BaseNativeSdkIntegrationTest {

  private final String connectorConfig =
      "{"
          + "\"warehouse\": \"xs\", "
          + "\"destination_database\": \"dest_db\", "
          + "\"destination_schema\": \"dest_schema\", "
          + "\"data_owner_role\": \"role\""
          + "}";

  @Test
  void shouldSaveConfigurationAndChangeConfigurationStatusToConfiguredIfItIsNotFinalized() {
    // given
    setConnectorStatus(CONFIGURING, INSTALLED);

    // when
    var response = configureConnector();

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertExternalStatus(CONFIGURING, CONFIGURED);
    assertSavedConfiguration();
  }

  private Map<String, Variant> configureConnector() {
    return callProcedure(format("CONFIGURE_CONNECTOR(PARSE_JSON('%s'))", connectorConfig));
  }

  private void assertSavedConfiguration() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'connector_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).isEqualTo(connectorConfig);
  }
}
