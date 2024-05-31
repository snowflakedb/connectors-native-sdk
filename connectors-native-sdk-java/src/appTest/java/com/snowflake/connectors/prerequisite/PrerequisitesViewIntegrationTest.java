/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.prerequisite;

import static com.snowflake.connectors.util.RowUtil.row;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PrerequisitesViewIntegrationTest extends BasePrerequisiteTest {

  @Test
  void shouldCreateAViewOnPrerequisitesTableAndAllowExecuteSelectOnIt() {
    var result = session.sql("SELECT * FROM PUBLIC.PREREQUISITES").collect();

    assertThat(result)
        .containsExactlyInAnyOrder(
            row("3", "example", "example", null, null, null, null, false),
            row("2", "example", "example", null, null, null, null, false),
            row("1", "example", "example", null, null, null, null, false));
  }
}
