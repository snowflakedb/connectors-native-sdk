/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.taskreactor.WorkSelectorType;
import com.snowflake.connectors.taskreactor.config.InMemoryConfigRepository;
import com.snowflake.connectors.taskreactor.queue.InMemoryWorkItemQueue;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class QueueItemSelectorTest {
  private final InMemoryConfigRepository configRepository = new InMemoryConfigRepository();
  private final InMemoryWorkItemQueue workItemQueue = new InMemoryWorkItemQueue();
  private final InMemoryWorkSelector workSelector = new InMemoryWorkSelector();

  private final QueueItemSelector queueItemSelector =
      new QueueItemSelector(configRepository, workItemQueue, workSelector);

  @AfterEach
  void afterEach() {
    workSelector.clear();
    configRepository.clear();
  }

  @Test
  void shouldSelectItemsFromView() {
    // given
    createConfiguration(WorkSelectorType.VIEW);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workSelector.addItems(List.of(queueItem1, queueItem2));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactlyInAnyOrder(queueItem1, queueItem2);
  }

  @Test
  void shouldSelectItemsFromProcedure() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    WorkItem workItem1 = WorkItem.from(queueItem1);
    WorkItem workItem2 = WorkItem.from(queueItem2);
    workSelector.addItems(List.of(queueItem1, queueItem2));
    workItemQueue.push(List.of(workItem1, workItem2));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactlyInAnyOrder(queueItem1, queueItem2);
    assertThat(workItemQueue.store())
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload)
        .containsExactlyInAnyOrder(
            Tuple.tuple(queueItem1.id, queueItem1.resourceId, queueItem1.workerPayload),
            Tuple.tuple(queueItem2.id, queueItem2.resourceId, queueItem2.workerPayload));
  }

  @Test
  void shouldRemoveFromQueueNotPickedItems() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    WorkItem workItem2 = WorkItem.from(queueItem2);
    workSelector.addItems(List.of(queueItem1, queueItem2));
    workItemQueue.push(List.of(workItem2));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactlyInAnyOrder(queueItem1, queueItem2);
    assertThat(workItemQueue.store())
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload)
        .containsExactlyInAnyOrder(
            Tuple.tuple(queueItem2.id, queueItem2.resourceId, queueItem2.workerPayload));
  }

  private static QueueItem generate(String id, String resourceId) {
    return QueueItem.fromMap(
        Map.of(
            "id", new Variant(id),
            "timestamp", new Variant(Timestamp.from(Instant.now())),
            "resourceId", new Variant(resourceId),
            "workerPayload", new Variant("payload")));
  }

  private void createConfiguration(WorkSelectorType workSelectorType) {
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC-test",
            "WORK_SELECTOR_TYPE", workSelectorType.name(),
            "WORK_SELECTOR", "work-selector-test",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "WAREHOUSE", "\"escap3d%_warehouse\""));
  }
}
