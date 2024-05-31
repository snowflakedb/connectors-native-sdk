/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.configuration;

import static com.snowflake.connectors.util.ConnectorStatus.CONFIGURING;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONFIGURED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.CONNECTED;
import static com.snowflake.connectors.util.ConnectorStatus.ConnectorConfigurationStatus.FINALIZED;
import static com.snowflake.connectors.util.ConnectorStatus.PAUSED;
import static com.snowflake.connectors.util.ResponseAssertions.assertThat;
import static java.lang.String.format;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class UpdateConnectionConfigurationTest extends BaseNativeSdkIntegrationTest {

  private static final Map<String, Object> connectionConfig =
      Map.of(
          "key1", "\"val1\".\"val1.5\"",
          "key2", "val2",
          "key3", "val3");

  private static final Map<String, Object> newConnectionConfig =
      Map.of(
          "key4", "\"val4\".\"val4.5\"",
          "key5", "val5",
          "key6", "val6");

  @Test
  void shouldReturnOkCommitNewConfigAndUpdateStatusToConnected() {
    // given
    setConnectorStatus(CONFIGURING, CONFIGURED);
    mockProcedure("TEST_CONNECTION()", "OK", "Connection test successful");
    mockProcedure(
        "TEST_DRAFT_CONNECTION(config VARIANT)", "OK", "Connection draft test successful");

    // when
    var responseSet = configureConnection();

    // then
    assertThat(responseSet).hasOkResponseCode();
    assertSavedConfiguration(connectionConfig);
    assertExternalStatus(CONFIGURING, CONNECTED);

    // when
    setConnectorStatus(PAUSED, FINALIZED);
    assertExternalStatus(PAUSED, FINALIZED);
    var responseUpdate = updateConnection();

    // then
    assertThat(responseUpdate).hasOkResponseCode();
    assertSavedConfiguration(newConnectionConfig);
    assertExternalStatus(PAUSED, FINALIZED);

    // cleanup
    dropProcedure("TEST_CONNECTION()");
    dropProcedure("TEST_DRAFT_CONNECTION(VARIANT)");
  }

  private Map<String, Variant> configureConnection() {
    var escapedConfig = new Variant(connectionConfig).asJsonString().replaceAll("\"", "\\\\\"");
    return callProcedure(format("SET_CONNECTION_CONFIGURATION(PARSE_JSON('%s'))", escapedConfig));
  }

  private Map<String, Variant> updateConnection() {
    var escapedConfig = new Variant(newConnectionConfig).asJsonString().replaceAll("\"", "\\\\\"");
    return callProcedure(
        format("UPDATE_CONNECTION_CONFIGURATION(PARSE_JSON('%s'))", escapedConfig));
  }

  private void assertSavedConfiguration(Map<String, Object> expectedConfig) {
    var savedConfig =
        callProcedure("GET_CONNECTION_CONFIGURATION()").entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asString()));
    Assertions.assertThat(savedConfig).isEqualTo(withOkResponse(expectedConfig));
  }

  private Map<String, Object> withOkResponse(Map<String, Object> base) {
    var baseCopy = new HashMap<>(base);
    baseCopy.put("response_code", "OK");
    return baseCopy;
  }
}
