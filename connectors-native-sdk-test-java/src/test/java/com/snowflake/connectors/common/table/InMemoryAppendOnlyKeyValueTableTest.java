/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.table;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryAppendOnlyKeyValueTableTest {

  AppendOnlyTable table;

  @BeforeEach
  void setup() {
    table = new InMemoryAppendOnlyKeyValueTable();
  }

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
