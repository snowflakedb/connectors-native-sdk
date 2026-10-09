/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static com.snowflake.connectors.taskreactor.WorkSelectorType.PROCEDURE;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.WorkSelectorType;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.config.TaskReactorConfig;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Class responsible for selection of items from the WorkItemQueue. */
public class QueueItemSelector {

  private final ConfigRepository configRepository;
  private final WorkItemQueue workItemQueue;
  private final WorkSelector workSelector;

  /**
   * Returns a new instance of the {@link QueueItemSelector QueueItemSelector}
   *
   * @param session Snowpark session object
   * @param instanceSchema Task Reactor schema instance
   * @return a new selector instance
   */
  public static QueueItemSelector from(Session session, Identifier instanceSchema) {
    return new QueueItemSelector(
        ConfigRepository.getInstance(session, instanceSchema),
        WorkItemQueue.getInstance(session, instanceSchema),
        WorkSelector.getInstance(session));
  }

  QueueItemSelector(
      ConfigRepository configRepository, WorkItemQueue workItemQueue, WorkSelector workSelector) {
    this.configRepository = configRepository;
    this.workItemQueue = workItemQueue;
    this.workSelector = workSelector;
  }

  /**
   * Selects items from WorkItemQueue based on Task Reactor configuration. Items are either selected
   * from the view or processed by the procedure provided during Task Reactor setup.
   *
   * <p>A procedure selector may only filter and reorder items already in the queue. Returned ids
   * that are not in the queue are ignored, and the stored queue row is dispatched so the procedure
   * cannot replace its payload.
   *
   * @return list of items to be dispatched
   */
  public List<QueueItem> getSelectedItemsFromQueue() {
    TaskReactorConfig config = configRepository.getConfig();
    WorkSelectorType workSelectorType = config.workSelectorType();
    String workSelectorName = config.workSelector();

    if (workSelectorType == PROCEDURE) {
      List<QueueItem> queueItems = workItemQueue.fetchNotProcessedAndCancelingItems();
      List<QueueItem> selectedQueueItems =
          workSelector.selectItemsUsingProcedure(queueItems, workSelectorName);
      return reconcileSelectedItems(queueItems, selectedQueueItems);
    }
    return workSelector.selectItemsFromView(workSelectorName);
  }

  private List<QueueItem> reconcileSelectedItems(
      List<QueueItem> originalQueueItems, List<QueueItem> selectedQueueItems) {
    Map<String, QueueItem> originalItemsById = new LinkedHashMap<>();
    for (QueueItem originalItem : originalQueueItems) {
      originalItemsById.putIfAbsent(originalItem.id, originalItem);
    }

    Set<String> acceptedIds = new LinkedHashSet<>();
    List<QueueItem> itemsToDispatch = new ArrayList<>();
    for (QueueItem selectedItem : selectedQueueItems) {
      if (selectedItem.id == null || !originalItemsById.containsKey(selectedItem.id)) {
        continue;
      }
      if (acceptedIds.add(selectedItem.id)) {
        itemsToDispatch.add(originalItemsById.get(selectedItem.id));
      }
    }

    // A non-empty response that matches no queued id is not a filter of the queue. Leave the rows
    // in place so a substituted batch cannot erase them, and dispatch nothing. An empty response
    // still means the selector filtered every row out.
    if (acceptedIds.isEmpty() && !selectedQueueItems.isEmpty()) {
      return List.of();
    }

    List<String> itemIdsToRemove =
        originalQueueItems.stream()
            .map(item -> item.id)
            .filter(not(acceptedIds::contains))
            .distinct()
            .collect(toList());
    workItemQueue.delete(itemIdsToRemove);
    return itemsToDispatch;
  }
}
