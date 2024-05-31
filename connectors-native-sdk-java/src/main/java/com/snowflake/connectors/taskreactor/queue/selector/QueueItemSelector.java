/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static com.snowflake.connectors.taskreactor.WorkSelectorType.PROCEDURE;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.WorkSelectorType;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.config.TaskReactorConfig;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.snowpark_java.Session;
import java.util.List;
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
      removeObsoleteQueueItems(queueItems, selectedQueueItems);
      return selectedQueueItems;
    }
    return workSelector.selectItemsFromView(workSelectorName);
  }

  private void removeObsoleteQueueItems(
      List<QueueItem> originalQueueItems, List<QueueItem> selectedQueueItems) {
    if (originalQueueItems.size() == selectedQueueItems.size()) {
      return;
    }

    Set<String> selectedItemIds = selectedQueueItems.stream().map(i -> i.id).collect(toSet());
    List<String> itemIdsToRemove =
        originalQueueItems.stream()
            .map(i -> i.id)
            .filter(not(selectedItemIds::contains))
            .collect(toList());
    workItemQueue.delete(itemIdsToRemove);
  }
}
