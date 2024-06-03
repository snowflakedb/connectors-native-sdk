/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.snowflake.snowpark_java.Row;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

public class ExamplePushBasedJavaConnectorTest extends BaseConnectorTest {

  private static final String RESOURCE = "INT_TEST";
  private static final String BASE_TABLE = RESOURCE + "_BASE";
  private static final String DELTA_TABLE = RESOURCE + "_DELTA";
  private static final String MERGED_VIEW = "VIEW_" + RESOURCE + "_MERGED";
  private static final String MERGE_TASK = DATABASE_NAME + ".TASKS.MERGE_" + RESOURCE + "_TASK";

  @Test
  void shouldPerformInitialAndPeriodicalUpload() {
    // when init resource procedure is called
    objectsInitializer.initResource(RESOURCE);
    // then elements are created
    assertThatTableHasElementCount(BASE_TABLE, 0);
    assertThatTableHasElementCount(DELTA_TABLE, 0);
    assertThatTableHasElementCount(MERGED_VIEW, 0);

    // suspend created task
    suspendTask(MERGE_TASK);

    // when initial upload is performed
    var records = dataSource.fetchInitialRecords(10);
    ingestionService.uploadRecords(BASE_TABLE, records).join();
    // then base table contains data
    assertThatTableHasElementCount(BASE_TABLE, 10);
    assertThatTableHasElementCount(DELTA_TABLE, 0);
    assertThatTableHasElementCount(MERGED_VIEW, 10);

    // when single iteration of periodical upload is performed
    var deltaRecords = dataSource.fetchDeltaRecordsWithIds(1, 2, 3, 11, 12, 13);
    ingestionService.uploadRecords(DELTA_TABLE, deltaRecords).join();
    // then delta table contains data
    assertThatTableHasElementCount(BASE_TABLE, 10);
    assertThatTableHasElementCount(DELTA_TABLE, 6);
    assertThatTableHasElementCount(MERGED_VIEW, 13);

    // when task which merges delta table to base table is executed
    executeTask(MERGE_TASK);

    // then base table contains more records and delta table is empty
    verifyAsync(
        () -> {
          assertThatTableHasElementCount(BASE_TABLE, 13);
          assertThatTableHasElementCount(DELTA_TABLE, 0);
          assertThatTableHasElementCount(MERGED_VIEW, 13);
        });
  }

  private void assertThatTableHasElementCount(String tableName, int expectedCount) {
    assertEquals(expectedCount, selectCountFromTable(tableName));
  }

  private int selectCountFromTable(String tableName) {
    Row[] result = session.sql(format("SELECT COUNT(*) FROM %s", tableName)).collect();
    return result[0].getInt(0);
  }

  private void suspendTask(String taskName) {
    session.sql(format("ALTER TASK %s SUSPEND", taskName)).collect();
  }

  private void executeTask(String taskName) {
    session.sql(format("EXECUTE TASK %s", taskName)).collect();
  }

  private void verifyAsync(Runnable assertion) {
    await()
        .atMost(ofMinutes(1))
        .with()
        .pollInterval(ofSeconds(5))
        .until(
            () -> {
              try {
                assertion.run();
                return true;
              } catch (AssertionFailedError e) {
                return false;
              }
            });
  }
}
