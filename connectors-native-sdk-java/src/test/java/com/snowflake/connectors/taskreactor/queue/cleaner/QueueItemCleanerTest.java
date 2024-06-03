/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.cleaner;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.taskreactor.config.InMemoryConfigRepository;
import com.snowflake.connectors.taskreactor.queue.InMemoryWorkItemQueue;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QueueItemCleanerTest {
  private final InMemoryConfigRepository configRepository = new InMemoryConfigRepository();
  private final InMemoryWorkItemQueue workItemQueue = new InMemoryWorkItemQueue();
  private final InMemoryExpiredWorkSelector expiredWorkSelector = new InMemoryExpiredWorkSelector();
  private final QueueItemCleaner queueItemCleaner =
      new QueueItemCleaner(expiredWorkSelector, configRepository, workItemQueue);

  @BeforeAll
  void beforeEach() {
    configRepository.updateConfig(
        Map.of(
            "SCHEMA", "schema-test",
            "WORKER_PROCEDURE", "TEST_PROC",
            "WORK_SELECTOR_TYPE", "PROCEDURE",
            "WORK_SELECTOR", "work-selector-test",
            "EXPIRED_WORK_SELECTOR", "expired-work-selector-test",
            "WAREHOUSE", "test-warehouse"));
  }

  @AfterEach
  void afterEach() {
    workItemQueue.clear();
    expiredWorkSelector.clear();
  }

  @Test
  void shouldLeaveAllUnexpiredItemsInTheQueue() {
    // given
    WorkItem workItem1 = new WorkItem("1", "2", new Variant("payload"));
    WorkItem workItem2 = new WorkItem("2", "2", new Variant("payload"));
    workItemQueue.push(List.of(workItem1, workItem2));

    // when
    queueItemCleaner.clean();

    // then
    assertThat(workItemQueue.store())
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload)
        .containsExactlyInAnyOrder(
            Tuple.tuple(workItem1.id, workItem1.resourceId, workItem1.payload),
            Tuple.tuple(workItem2.id, workItem2.resourceId, workItem2.payload));
  }

  @Test
  void shouldRemoveExpiredItemsFromTheQueue() {
    // given
    WorkItem workItem1 = new WorkItem("1", "2", new Variant("payload"));
    WorkItem workItem2 = new WorkItem("2", "2", new Variant("payload"));
    workItemQueue.push(List.of(workItem1, workItem2));
    expiredWorkSelector.addIdentifiers(List.of("1", "3"));

    // when
    queueItemCleaner.clean();

    // then
    assertThat(workItemQueue.store())
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload)
        .containsExactly(Tuple.tuple(workItem2.id, workItem2.resourceId, workItem2.payload));
  }
}
