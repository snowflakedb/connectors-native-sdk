/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WorkerQueueTest extends BaseIntegrationTest {

  private static final Identifier schema = Identifier.from("TEST_SCHEMA");
  private static final WorkerId workerId = new WorkerId(1);
  private static String tableName;
  private static WorkerQueue workerQueue;

  @BeforeAll
  void beforeAll() {
    workerQueue = WorkerQueue.getInstance(session, schema);
    tableName = String.format("%s.%s.%s", DATABASE_NAME, schema.getValue(), "WORKER_QUEUE_1");
    session
        .sql(String.format("CREATE SCHEMA %s.%s", DATABASE_NAME, schema.getValue()))
        .toLocalIterator();
    session
        .sql(
            String.format(
                "CREATE TABLE %s (ID STRING, RESOURCE_ID STRING, WORKER_PAYLOAD VARIANT)",
                tableName))
        .toLocalIterator();
  }

  @AfterEach
  void afterEach() {
    session.sql(String.format("TRUNCATE TABLE %s", tableName)).toLocalIterator();
  }

  @AfterAll
  void afterAll() {
    session.sql(String.format("DROP TABLE %s", tableName)).toLocalIterator();
    session
        .sql(String.format("DROP SCHEMA %s.%s", DATABASE_NAME, schema.getValue()))
        .toLocalIterator();
  }

  @Test
  void shouldFetchItemFromQueue() {
    // given
    session.sql(
        String.format(
            "INSERT INTO %s SELECT '%s', '%s', PARSE_JSON('%s')", tableName, "1", "1", "payload"));
    QueueItem queueItem = generateQueueItem("1", "1");

    // when
    workerQueue.push(queueItem, workerId);

    // then
    WorkItem result = workerQueue.fetch(workerId);
    assertThat(result)
        .extracting(i -> i.id, i -> i.resourceId, i -> i.payload)
        .containsExactly("1", "1", new Variant("payload"));
  }

  @Test
  void shouldPushItemToWorkItem() {
    // given
    QueueItem queueItem = generateQueueItem("1", "1");

    // when
    workerQueue.push(queueItem, workerId);

    // then
    WorkItem result = workerQueue.fetch(workerId);
    assertThat(result)
        .extracting(i -> i.id, i -> i.resourceId, i -> i.payload)
        .containsExactly(queueItem.id, queueItem.resourceId, queueItem.workerPayload);
  }

  @Test
  void shouldFailToFetchWhenThereAreMultipleItems() {
    // given
    QueueItem queueItem1 = generateQueueItem("1", "1");
    QueueItem queueItem2 = generateQueueItem("2", "2");
    workerQueue.push(queueItem1, workerId);
    workerQueue.push(queueItem2, workerId);

    // when
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> workerQueue.fetch(workerId))
        .withMessage("Invalid work items number (2) on the queue");
  }

  private static QueueItem generateQueueItem(String id, String resourceId) {
    return QueueItem.fromMap(
        Map.of(
            "id", new Variant(id),
            "timestamp", new Variant(Timestamp.from(Instant.now())),
            "resourceId", new Variant(resourceId),
            "workerPayload", new Variant("payload")));
  }
}
