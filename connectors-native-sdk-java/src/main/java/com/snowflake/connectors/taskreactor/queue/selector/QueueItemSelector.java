/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static com.snowflake.connectors.taskreactor.WorkSelectorType.PROCEDURE;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.WorkSelectorType;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.config.TaskReactorConfig;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;

/** Class responsible for selection of items from the WorkItemQueue. */
public class QueueItemSelector {

  private static final Logger LOG = TaskReactorLogger.getLogger(QueueItemSelector.class);

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
   * <p>A selector may only filter and reorder items already in the queue. Returned ids that are not
   * in the queue are ignored, and the stored queue row is dispatched so the selector cannot replace
   * its payload. Cancellation markers are never dispatched as work. Rows with a null or duplicate
   * id are skipped so the rest of the queue can still be dispatched. An empty or fully unrecognized
   * response leaves real work queued. A procedure response still removes cancellation markers.
   *
   * @return list of items to be dispatched
   */
  public List<QueueItem> getSelectedItemsFromQueue() {
    TaskReactorConfig config = configRepository.getConfig();
    WorkSelectorType workSelectorType = config.workSelectorType();
    String workSelectorName = config.workSelector();
    List<QueueItem> queueItems = workItemQueue.fetchNotProcessedAndCancelingItems();
    Map<String, QueueItem> originalItemsById = indexQueueItems(queueItems);

    if (workSelectorType == PROCEDURE) {
      List<QueueItem> selectedQueueItems =
          workSelector.selectItemsUsingProcedure(queueItems, workSelectorName);
      return reconcileSelectedItems(originalItemsById, selectedQueueItems, UnselectedItems.REMOVE);
    }
    List<QueueItem> selectedQueueItems = workSelector.selectItemsFromView(workSelectorName);
    return reconcileSelectedItems(originalItemsById, selectedQueueItems, UnselectedItems.KEEP);
  }

  private List<QueueItem> reconcileSelectedItems(
      Map<String, QueueItem> originalItemsById,
      List<QueueItem> selectedQueueItems,
      UnselectedItems unselectedItems) {
    Set<String> acceptedIds = new LinkedHashSet<>();
    List<QueueItem> itemsToDispatch = new ArrayList<>();
    int unrecognizedIds = 0;
    int cancellationMarkers = 0;
    for (QueueItem selectedItem : selectedQueueItems) {
      if (selectedItem == null || selectedItem.id == null) {
        unrecognizedIds++;
        continue;
      }
      QueueItem originalItem = originalItemsById.get(selectedItem.id);
      if (originalItem == null) {
        unrecognizedIds++;
        continue;
      }
      if (originalItem.cancelOngoingExecution) {
        cancellationMarkers++;
        continue;
      }
      if (acceptedIds.add(selectedItem.id)) {
        itemsToDispatch.add(originalItem);
      }
    }

    if (unrecognizedIds > 0) {
      LOG.warn("Ignored {} selector item(s) whose ids are not in the queue.", unrecognizedIds);
    }
    if (cancellationMarkers > 0) {
      LOG.warn("Ignored {} cancellation marker(s) returned by the selector.", cancellationMarkers);
    }

    // no dispatchable ids: keep real work, dispatch nothing
    if (acceptedIds.isEmpty()) {
      if (unselectedItems == UnselectedItems.REMOVE) {
        removeCancellationMarkers(originalItemsById);
      }
      LOG.warn(
          "Selector response contained no dispatchable queue items (returned={}).",
          selectedQueueItems.size());
      return itemsToDispatch;
    }

    if (unselectedItems == UnselectedItems.REMOVE) {
      List<String> itemIdsToRemove =
          originalItemsById.keySet().stream().filter(not(acceptedIds::contains)).collect(toList());
      workItemQueue.delete(itemIdsToRemove);
    }
    return itemsToDispatch;
  }

  private void removeCancellationMarkers(Map<String, QueueItem> originalItemsById) {
    List<String> markerIds =
        originalItemsById.values().stream()
            .filter(item -> item.cancelOngoingExecution)
            .map(item -> item.id)
            .collect(toList());
    workItemQueue.delete(markerIds);
  }

  private static Map<String, QueueItem> indexQueueItems(List<QueueItem> originalQueueItems) {
    Map<String, QueueItem> originalItemsById = new LinkedHashMap<>();
    Set<String> duplicateIds = new LinkedHashSet<>();
    int nullItems = 0;
    int nullIds = 0;
    for (QueueItem originalItem : originalQueueItems) {
      if (originalItem == null) {
        nullItems++;
        continue;
      }
      if (originalItem.id == null) {
        nullIds++;
        continue;
      }
      QueueItem previous = originalItemsById.putIfAbsent(originalItem.id, originalItem);
      if (previous != null) {
        duplicateIds.add(originalItem.id);
      }
    }
    duplicateIds.forEach(originalItemsById::remove);
    if (nullItems > 0 || nullIds > 0 || !duplicateIds.isEmpty()) {
      LOG.error(
          "Skipped corrupted queue items (nullItems={}, nullIds={}, duplicateIds={}).",
          nullItems,
          nullIds,
          duplicateIds);
    }
    return originalItemsById;
  }

  private enum UnselectedItems {
    REMOVE,
    KEEP
  }
}
