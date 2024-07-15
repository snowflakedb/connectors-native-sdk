/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.STARTED;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FinalizeConnectorConfigurationIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final String customConfig = "{\"journal_table\": \"j_table_name\"}";

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();
    mockProcedureWithBody(
        "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL(config VARIANT)",
        "INSERT INTO STATE.APP_CONFIG SELECT \\'custom_configuration\\', :config, "
            + "current_timestamp(); RETURN OBJECT_CONSTRUCT(\\'response_code\\', \\'OK\\')");
  }

  @Test
  void shouldChangeStatusToStartedAndFinalized() {
    // given
    setConnectorStatus(CONFIGURING, CONNECTED);

    // when
    var response = finalizeConfiguration();

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertExternalStatus(STARTED, FINALIZED);
    assertSavedConfiguration();
  }

  private Map<String, Variant> finalizeConfiguration() {
    var query = format("FINALIZE_CONNECTOR_CONFIGURATION(PARSE_JSON('%s'))", customConfig);
    return callProcedure(query);
  }

  private void assertSavedConfiguration() {
    var query = "SELECT value FROM STATE.APP_CONFIG WHERE key = 'custom_configuration'";
    var savedConfig = executeInApp(query)[0].getString(0);
    JsonAssertions.assertThatJson(savedConfig).isEqualTo(customConfig);
  }
}
