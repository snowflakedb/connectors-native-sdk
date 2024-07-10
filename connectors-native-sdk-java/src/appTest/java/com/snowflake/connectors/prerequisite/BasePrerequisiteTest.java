/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.prerequisite;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThatResponseMap;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.snowpark_java.Row;
import java.io.IOException;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

class BasePrerequisiteTest extends BaseNativeSdkIntegrationTest {

  @BeforeAll
  @Override
  public void beforeAll() throws IOException {
    super.beforeAll();
    executeInApp("TRUNCATE TABLE IF EXISTS STATE.PREREQUISITES");
    executeInApp(
        "INSERT INTO STATE.PREREQUISITES (ID, TITLE, DESCRIPTION, POSITION) VALUES "
            + " ('1', 'example', 'example', 3), "
            + " ('2', 'example', 'example', 2), "
            + " ('3', 'example', 'example', 1)");
  }

  protected void assertPrerequisitesTableContent(Row... expectedContent) {
    var dbContent = session.sql("SELECT id, is_completed FROM PUBLIC.PREREQUISITES").collect();
    Assertions.assertThat(dbContent).containsExactlyInAnyOrder(expectedContent);
  }

  protected void markAllPrerequisitesAsUndone() {
    executeInApp("UPDATE STATE.PREREQUISITES SET is_completed = FALSE");
  }

  protected void markAllPrerequisitesAsDone() {
    var result = callProcedure("MARK_ALL_PREREQUISITES_AS_DONE()");
    assertThatResponseMap(result).hasOKResponseCode();
  }

  protected void updateIndexedPrerequisites(List<Boolean> values) {
    for (int i = 1; i <= values.size(); i++) {
      boolean value = values.get(i - 1);
      var result = callProcedure(String.format("UPDATE_PREREQUISITE('%s', %s)", i, value));
      assertThatResponseMap(result).hasOKResponseCode();
    }
  }
}
