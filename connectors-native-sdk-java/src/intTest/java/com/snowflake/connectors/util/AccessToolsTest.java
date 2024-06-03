/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.util.snowflake.AccessTools;
import org.junit.jupiter.api.Test;

public class AccessToolsTest extends BaseIntegrationTest {

  private final AccessTools accessTools = AccessTools.getInstance(session);

  @Test
  void shouldHaveAccessToXSWarehouse() {
    // expect
    assertThat(accessTools.hasWarehouseAccess(Identifier.from(WAREHOUSE_NAME))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToNonexistentWarehouse() {
    // expect
    assertThat(accessTools.hasWarehouseAccess(Identifier.from("NONEXISTENT_WH"))).isFalse();
  }

  @Test
  void shouldHaveAccessToTheStateSchema() {
    // expect
    assertThat(accessTools.hasSchemaAccess(Identifier.from("STATE"))).isTrue();
  }

  @Test
  void shouldNotHaveAccessToNonexistentSchema() {
    // expect
    assertThat(accessTools.hasSchemaAccess(Identifier.from("NONEXISTENT_SCHEMA"))).isFalse();
  }
}
