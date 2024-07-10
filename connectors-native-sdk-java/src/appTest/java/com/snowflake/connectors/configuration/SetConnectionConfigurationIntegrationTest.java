/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;
import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SetConnectionConfigurationIntegrationTest extends BaseNativeSdkIntegrationTest {

  private static final Map<String, Object> connectionConfig =
      Map.of(
          "key1", "\"val1\".\"val1.5\"",
          "key2", "val2",
          "key3", "val3");

  @Test
  void shouldReturnOkCommitNewConfigAndUpdateStatusToConnected() {
    // given
    setConnectorStatus(CONFIGURING, CONFIGURED);
    mockProcedure("TEST_CONNECTION()", "OK", "Connection test successful");

    // when
    var response = configureConnection();

    // then
    assertThatResponseMap(response).hasOKResponseCode();
    assertSavedConfiguration();
    assertExternalStatus(CONFIGURING, CONNECTED);

    // cleanup
    dropProcedure("TEST_CONNECTION()");
  }

  private Map<String, Variant> configureConnection() {
    return configureConnection(connectionConfig);
  }

  private Map<String, Variant> configureConnection(Map<String, Object> savedConfig) {
    var escapedConfig = new Variant(savedConfig).asJsonString().replaceAll("\"", "\\\\\"");
    return callProcedure(format("SET_CONNECTION_CONFIGURATION(PARSE_JSON('%s'))", escapedConfig));
  }

  private Map<String, Object> withOkResponse(Map<String, Object> base) {
    var baseCopy = new HashMap<>(base);
    baseCopy.put("response_code", "OK");
    return baseCopy;
  }

  private void assertSavedConfiguration() {
    assertSavedConfiguration(connectionConfig);
  }

  private void assertSavedConfiguration(Map<String, Object> expectedConfig) {
    var savedConfig =
        callProcedure("GET_CONNECTION_CONFIGURATION()").entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asString()));
    assertThat(savedConfig).isEqualTo(withOkResponse(expectedConfig));
  }
}
