/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.snowpark_java.types.Variant;
import org.junit.jupiter.api.Test;

public class AppendOnlyKeyValueTableTest extends BaseIntegrationTest {

  AppendOnlyTable table = new AppendOnlyKeyValueTable(session, "STATE.RESOURCE_INGESTION_STATE");

  @Test
  void shouldAccessIntConfiguration() {
    // when
    table.insert("key", new Variant(17));

    // then
    assertThat(table.fetch("key")).isEqualTo(new Variant(17));
  }

  @Test
  void shouldAccessLatestStringConfiguration() {
    // when
    table.insert("key2", new Variant("test value"));
    table.insert("key1", new Variant("test value two"));
    table.insert("key2", new Variant("test value three"));

    // then
    assertThat(table.fetch("key1")).isEqualTo(new Variant("test value two"));
    assertThat(table.fetch("key2")).isEqualTo(new Variant("test value three"));
  }
}
