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
   * its payload. Cancellation markers are never dispatched as work. An empty or fully unrecognized
   * response leaves the queue unchanged.
   *
   * @return list of items to be dispatched
   */
  public List<QueueItem> getSelectedItemsFromQueue() {
    TaskReactorConfig config = configRepository.getConfig();
    WorkSelectorType workSelectorType = config.workSelectorType();
    String workSelectorName = config.workSelector();
    List<QueueItem> queueItems = workItemQueue.fetchNotProcessedAndCancelingItems();

    if (workSelectorType == PROCEDURE) {
      List<QueueItem> selectedQueueItems =
          workSelector.selectItemsUsingProcedure(queueItems, workSelectorName);
      return reconcileSelectedItems(queueItems, selectedQueueItems, true);
    }
    List<QueueItem> selectedQueueItems = workSelector.selectItemsFromView(workSelectorName);
    return reconcileSelectedItems(queueItems, selectedQueueItems, false);
  }

  private List<QueueItem> reconcileSelectedItems(
      List<QueueItem> originalQueueItems,
      List<QueueItem> selectedQueueItems,
      boolean removeUnselected) {
    Map<String, QueueItem> originalItemsById = indexQueueItems(originalQueueItems);

    Set<String> acceptedIds = new LinkedHashSet<>();
    int unrecognizedIds = 0;
    for (QueueItem selectedItem : selectedQueueItems) {
      QueueItem originalItem =
          selectedItem.id == null ? null : originalItemsById.get(selectedItem.id);
      if (originalItem == null) {
        unrecognizedIds++;
        continue;
      }
      if (originalItem.cancelOngoingExecution) {
        continue;
      }
      acceptedIds.add(selectedItem.id);
    }

    if (unrecognizedIds > 0) {
      LOG.warn("Ignored {} selector item(s) whose ids are not in the queue.", unrecognizedIds);
    }

    List<QueueItem> itemsToDispatch = new ArrayList<>();
    for (String acceptedId : acceptedIds) {
      itemsToDispatch.add(originalItemsById.get(acceptedId));
    }

    // no dispatchable ids: keep queue, dispatch nothing
    if (acceptedIds.isEmpty()) {
      LOG.warn(
          "Selector response contained no dispatchable queue items (returned={}). Leaving the queue"
              + " unchanged.",
          selectedQueueItems.size());
      return itemsToDispatch;
    }

    if (removeUnselected) {
      List<String> itemIdsToRemove =
          originalItemsById.keySet().stream().filter(not(acceptedIds::contains)).collect(toList());
      workItemQueue.delete(itemIdsToRemove);
    }
    return itemsToDispatch;
  }

  private static Map<String, QueueItem> indexQueueItems(List<QueueItem> originalQueueItems) {
    Map<String, QueueItem> originalItemsById = new LinkedHashMap<>();
    for (QueueItem originalItem : originalQueueItems) {
      if (originalItem.id == null) {
        throw new IllegalStateException("Queue item id must not be null");
      }
      QueueItem previous = originalItemsById.putIfAbsent(originalItem.id, originalItem);
      if (previous != null) {
        throw new IllegalStateException("Duplicate queue item id: " + originalItem.id);
      }
    }
    return originalItemsById;
  }
}
