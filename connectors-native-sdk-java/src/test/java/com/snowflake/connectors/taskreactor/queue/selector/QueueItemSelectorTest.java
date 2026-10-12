/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result)
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(tuple("1", "1", "payload"), tuple("2", "2", "payload"));
  }

  @Test
  void shouldNotRemoveUnselectedItemsWhenSelectingFromView() {
    // given
    createConfiguration(WorkSelectorType.VIEW);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workSelector.addItems(List.of(queueItem1));
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1", "2");
  }

  @Test
  void shouldDispatchStoredRowWhenViewRewritesPayload() {
    // given
    createConfiguration(WorkSelectorType.VIEW);
    QueueItem stored = generate("1", "resource-1", "real-payload");
    QueueItem rewritten = generate("1", "evil-resource", "evil-payload");
    workSelector.addItems(List.of(rewritten));
    workItemQueue.push(List.of(WorkItem.from(stored)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(result.get(0).workerPayload.asString()).isEqualTo("real-payload");
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
    assertThat(result)
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(tuple("1", "1", "payload"), tuple("2", "2", "payload"));
  }

  @Test
  void shouldRemoveFromQueueNotPickedItems() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workSelector.addItems(List.of(queueItem1));
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1");
  }

  @Test
  void shouldPreserveSelectorOrder() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workSelector.addItems(List.of(queueItem2, queueItem1));
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("2"), storedItem("1"));
  }

  @Test
  void shouldDispatchStoredRowWhenSelectorRewritesPayload() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem stored = generate("1", "resource-1", "real-payload");
    QueueItem rewritten = generate("1", "evil-resource", "evil-payload");
    workSelector.addItems(List.of(rewritten));
    workItemQueue.push(List.of(WorkItem.from(stored)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(result)
        .extracting(item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(Tuple.tuple("resource-1", "real-payload"));
  }

  @Test
  void shouldKeepQueueWhenSelectorReturnsNone() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).isEmpty();
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1", "2");
  }

  @Test
  void shouldDispatchOtherItemsWhenQueueContainsDuplicateIds() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    workItemQueue.push(
        List.of(
            new WorkItem("same", "resource-1", new Variant("one")),
            new WorkItem("same", "resource-2", new Variant("two")),
            new WorkItem("ok", "resource-3", new Variant("three"))));
    workSelector.addItems(
        List.of(generate("ok", "resource-3", "three"), generate("same", "resource-1", "one")));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result)
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(tuple("ok", "resource-3", "three"));
    assertThat(workItemQueue.store())
        .extracting(item -> item.id)
        .containsExactly("same", "same", "ok");
  }

  @Test
  void shouldDispatchOtherItemsWhenQueueItemIdIsNull() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    workItemQueue.push(
        List.of(
            new WorkItem(null, "resource-null", new Variant("skip")),
            new WorkItem("ok", "resource-ok", new Variant("keep"))));
    workSelector.addItems(List.of(generate("ok", "resource-ok", "keep")));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result)
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(tuple("ok", "resource-ok", "keep"));
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly(null, "ok");
  }

  @Test
  void shouldIgnoreViewItemsThatAreNotQueued() {
    // given
    createConfiguration(WorkSelectorType.VIEW);
    QueueItem queued = generate("1", "resource-1", "real-payload");
    workItemQueue.push(List.of(WorkItem.from(queued)));
    workSelector.addItems(
        List.of(generate("other", "evil", "injected"), generate("1", "evil", "injected")));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result)
        .extracting(item -> item.id, item -> item.resourceId, item -> item.workerPayload.asString())
        .containsExactly(tuple("1", "resource-1", "real-payload"));
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1");
  }

  @Test
  void shouldRemoveCancellationMarkersWhenSelectorReturnsNone() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem = generate("1", "resource-1");
    workItemQueue.push(List.of(WorkItem.from(queueItem)));
    workItemQueue.cancelOngoingExecutions(List.of("resource-1"));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).isEmpty();
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1");
    assertThat(workItemQueue.store()).noneMatch(item -> item.cancelOngoingExecution);
  }

  @Test
  void shouldNotDispatchCancellationMarker() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem = generate("1", "resource-1");
    workItemQueue.push(List.of(WorkItem.from(queueItem)));
    workItemQueue.cancelOngoingExecutions(List.of("resource-1"));
    QueueItem cancellationMarker =
        workItemQueue.store().stream()
            .filter(item -> item.cancelOngoingExecution)
            .findFirst()
            .orElseThrow();
    workSelector.addItems(List.of(queueItem, cancellationMarker));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(result).noneMatch(item -> item.cancelOngoingExecution);
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1");
  }

  @Test
  void shouldDropFabricatedItemsWhenBatchSizeMatches() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    QueueItem fabricated1 = generate("fake-1", "evil-1", "injected");
    QueueItem fabricated2 = generate("fake-2", "evil-2", "injected");
    workSelector.addItems(List.of(fabricated1, fabricated2));
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).isEmpty();
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1", "2");
  }

  @Test
  void shouldDispatchKnownIdOnceWhenSelectorRepeatsIt() {
    // given
    createConfiguration(WorkSelectorType.PROCEDURE);
    QueueItem queueItem1 = generate("1", "1");
    QueueItem queueItem2 = generate("2", "2");
    workSelector.addItems(List.of(queueItem1, queueItem1));
    workItemQueue.push(List.of(WorkItem.from(queueItem1), WorkItem.from(queueItem2)));

    // when
    List<QueueItem> result = queueItemSelector.getSelectedItemsFromQueue();

    // then
    assertThat(result).containsExactly(storedItem("1"));
    assertThat(workItemQueue.store()).extracting(item -> item.id).containsExactly("1");
  }

  private QueueItem storedItem(String id) {
    return workItemQueue.store().stream()
        .filter(item -> id.equals(item.id))
        .findFirst()
        .orElseThrow();
  }

  private static QueueItem generate(String id, String resourceId) {
    return generate(id, resourceId, "payload");
  }

  private static QueueItem generate(String id, String resourceId, String payload) {
    return QueueItem.fromMap(
        Map.of(
            "id", new Variant(id),
            "timestamp", new Variant(Timestamp.from(Instant.now())),
            "resourceId", new Variant(resourceId),
            "workerPayload", new Variant(payload)));
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
