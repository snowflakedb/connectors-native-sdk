/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import static com.snowflake.connectors.common.IdGenerator.randomId;
import static java.util.function.Predicate.not;

import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** In memory implementation of {@link WorkItemQueue}. */
public class InMemoryWorkItemQueue implements WorkItemQueue {

  private final List<QueueItem> store = new ArrayList<>();

  @Override
  public List<QueueItem> fetchNotProcessedAndCancelingItems() {
    return List.copyOf(store);
  }

  @Override
  public void push(List<WorkItem> workItems) {
    var mappedItems =
        workItems.stream()
            .map(
                item ->
                    new QueueItem(
                        item.id,
                        Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)),
                        item.resourceId,
                        false,
                        item.payload))
            .collect(Collectors.toList());
    store.addAll(mappedItems);
  }

  @Override
  public void cancelOngoingExecutions(List<String> ids) {
    var cancelingItems =
        ids.stream()
            .map(
                id ->
                    new QueueItem(
                        randomId(),
                        Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)),
                        id,
                        true,
                        new Variant("")))
            .collect(Collectors.toList());
    store.addAll(cancelingItems);
  }

  @Override
  public void delete(List<String> ids) {
    var itemsToDelete =
        store.stream().filter(item -> ids.contains(item.id)).collect(Collectors.toList());

    store.removeAll(itemsToDelete);
  }

  @Override
  public void deleteBefore(String resourceId, Timestamp timestamp) {
    var itemsToDelete =
        store.stream()
            .filter(item -> item.resourceId.equals(resourceId))
            .filter(not(item -> item.timestamp.after(timestamp)))
            .collect(Collectors.toList());

    store.removeAll(itemsToDelete);
  }

  /**
   * Returns the list backing this queue.
   *
   * @return list backing this queue
   */
  public List<QueueItem> store() {
    return store;
  }

  /** Clears this queue. */
  public void clear() {
    store.clear();
  }
}
