/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.state;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.assertions.common.state.TestState;
import com.snowflake.connectors.common.table.AppendOnlyKeyValueTable;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DefaultAppendOnlyStateRepositoryTest extends BaseIntegrationTest {

  @Test
  void shouldStoreAndReadConfigurationBasedOnCustomClass() {
    // given
    AppendOnlyKeyValueTable table =
        new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");
    DefaultAppendOnlyStateRepository<TestState> repo =
        new DefaultAppendOnlyStateRepository<>(table, TestState.class);
    TestState config = new TestState(true, "text value", 127);

    // when
    repo.insert("my_config", config);
    TestState result = repo.fetch("my_config");

    // then
    assertThat(result)
        .isTestStateObject()
        .hasFlag(true)
        .hasStringState("text value")
        .hasIntState(127);
  }

  @Test
  void shouldStoreAndReadConfigurationBasedOnVariantClass() {
    // given
    AppendOnlyKeyValueTable table =
        new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");
    DefaultAppendOnlyStateRepository<Variant> repo =
        new DefaultAppendOnlyStateRepository<>(table, Variant.class);
    Variant variant = new Variant(Map.of("state", "enabled", "age", 18));

    // when
    repo.insert("my_variant", variant);
    Variant result = repo.fetch("my_variant");

    // then
    assertThat(result).isEqualTo(variant);
  }

  @Test
  void shouldStoreAndReadStringConfiguration() {
    // given
    AppendOnlyKeyValueTable table =
        new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");
    DefaultAppendOnlyStateRepository<String> repo =
        new DefaultAppendOnlyStateRepository<>(table, String.class);
    String config = "my text";

    // when
    repo.insert("my_string_config", config);
    String result = repo.fetch("my_string_config");

    // then
    assertThat(result).isEqualTo("my text");
  }

  @Test
  void shouldStoreAndReadIntConfiguration() {
    // given
    AppendOnlyKeyValueTable table =
        new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");
    DefaultAppendOnlyStateRepository<Integer> repo =
        new DefaultAppendOnlyStateRepository<Integer>(table, Integer.class);
    int config = 12312;

    // when
    repo.insert("my_string_config", config);
    Integer result = repo.fetch("my_string_config");

    // then
    assertThat(result).isEqualTo(12312);
  }

  @Test
  void shouldStoreAndReadLatestConfigurationBasedOnCustomClass() {
    // given
    AppendOnlyKeyValueTable table =
        new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");
    DefaultAppendOnlyStateRepository<TestState> repo =
        new DefaultAppendOnlyStateRepository<>(table, TestState.class);

    TestState config1 = new TestState(true, "text value one", 127);
    TestState config2 = new TestState(true, "text value two", 128);
    TestState config3 = new TestState(true, "text value three", 129);

    // when
    repo.insert("my_config", config1);
    repo.insert("my_other_config", config2);
    repo.insert("my_config", config3);
    TestState actualFirstState = repo.fetch("my_config");
    TestState actualSecondState = repo.fetch("my_other_config");

    // then
    assertThat(actualFirstState)
        .isTestStateObject()
        .hasFlag(true)
        .hasStringState("text value three")
        .hasIntState(129);
    assertThat(actualSecondState)
        .isTestStateObject()
        .hasFlag(true)
        .hasStringState("text value two")
        .hasIntState(128);
  }
}
