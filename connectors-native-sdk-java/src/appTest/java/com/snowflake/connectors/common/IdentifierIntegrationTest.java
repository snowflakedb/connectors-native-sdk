/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class IdentifierIntegrationTest extends BaseNativeSdkIntegrationTest {

  @BeforeAll
  @Override
  public void beforeAll() throws IOException {
    super.beforeAll();
    createWarehouse("TEST_WH");
    createWarehouse("\"TEST_w'h_QUOTED\"\"@$❄\"");
  }

  @Test
  void shouldSaveIdentifiersInTheConnectorConfiguration() {
    // when
    configureConnector();

    // then
    var configValues =
        getConfigValues(
            "connector_configuration",
            new String[] {"warehouse", "destination_database", "destination_schema"});
    configValues.assertContainsExactly("xsmall", "dEsT_Db", "\"dest\"\"sch'ema_@$%\"");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TEST_WH", "\"TEST_w'h_QUOTED\"\"@$❄\""})
  void shouldUpdateWarehouseIdentifierInTheConnectorConfiguration(String warehouse) {
    // given
    configureConnector();
    setConnectorStatus(PAUSED, FINALIZED);

    // when
    updateWarehouse(warehouse);

    // then
    var configValues = getConfigValues("connector_configuration", "warehouse");
    configValues.assertContainsExactly(warehouse);
  }

  @Test
  void shouldSaveCustomConfiguration() {
    // given
    setConnectorStatus(CONFIGURING, CONNECTED);
    mockProcedureWithHandler(
        "FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL(config VARIANT)",
        "com.snowflake.connectors.appTest.FinalizeConnectorConfigurationInternalMock.execute");

    // when
    finalizeConfiguration();

    // then
    var selectedKeys =
        new String[] {
          "unquoted_identifier", "quoted_identifier", "unquoted_obj_name", "quoted_obj_name"
        };
    var expectedValues =
        new String[] {
          "xsmall", "\"QuoT3d_$%\"\"❄\"", "db.schema.name", "\"a.b.c\".schema.\"N4m'$%❄\""
        };

    getConfigValues("mock_custom_config_src", selectedKeys).assertContainsExactly(expectedValues);
    getConfigValues("mock_custom_config_modified", selectedKeys)
        .assertContainsExactly(expectedValues);

    // cleanup
    mockProcedure("FINALIZE_CONNECTOR_CONFIGURATION_INTERNAL(config VARIANT)", "OK", null);
  }

  private void configureConnector() {
    var connectorConfig =
        Map.of(
            "warehouse", "xsmall",
            "destination_database", "dEsT_Db",
            "destination_schema", "\"dest\"\"sch'ema_@$%\"");
    callProcedure(format("CONFIGURE_CONNECTOR(%s)", asVariant(new Variant(connectorConfig))));
  }

  private void finalizeConfiguration() {
    var customConfig =
        Map.of(
            "unquoted_identifier", "xsmall",
            "quoted_identifier", "\"QuoT3d_$%\"\"❄\"",
            "unquoted_obj_name", "db.schema.name",
            "quoted_obj_name", "\"a.b.c\".schema.\"N4m'$%❄\"");
    callProcedure(
        format("FINALIZE_CONNECTOR_CONFIGURATION(%s)", asVariant(new Variant(customConfig))));
  }

  private void updateWarehouse(String warehouse) {
    callProcedure(format("UPDATE_WAREHOUSE(%s)", asVarchar(warehouse)));
  }

  private ConfigValues getConfigValues(String configKey, String[] jsonKeys) {
    var selectedHierarchicalValues =
        Arrays.stream(jsonKeys)
            .map(key -> format("value:%s::STRING", key))
            .collect(Collectors.joining(", "));
    var query = "SELECT value, %s FROM STATE.APP_CONFIG WHERE key = %s";

    var selectedRow =
        executeInApp(format(query, selectedHierarchicalValues, asVarchar(configKey)))[0];
    var selectedVariant = selectedRow.getVariant(0).asMap();

    var valuesFromHierarchicalQuery =
        IntStream.range(1, jsonKeys.length + 1)
            .mapToObj(selectedRow::getString)
            .toArray(String[]::new);
    var valuesFromVariantMapping =
        Arrays.stream(jsonKeys)
            .map(key -> selectedVariant.get(key).asString())
            .toArray(String[]::new);

    return new ConfigValues(valuesFromHierarchicalQuery, valuesFromVariantMapping);
  }

  private ConfigValues getConfigValues(String configKey, String jsonKey) {
    return getConfigValues(configKey, new String[] {jsonKey});
  }

  private static class ConfigValues {

    private final String[] fromHierarchicalQuery;
    private final String[] fromVariantMapping;

    private ConfigValues(String[] fromHierarchicalQuery, String[] fromVariantMapping) {
      this.fromHierarchicalQuery = fromHierarchicalQuery;
      this.fromVariantMapping = fromVariantMapping;
    }

    public void assertContainsExactly(String... expected) {
      assertThat(fromHierarchicalQuery).containsExactly(expected);
      assertThat(fromVariantMapping).containsExactly(expected);
    }
  }
}
