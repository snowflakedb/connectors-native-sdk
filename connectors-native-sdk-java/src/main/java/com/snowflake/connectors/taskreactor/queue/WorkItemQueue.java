/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.Session;
import java.sql.Timestamp;
import java.util.List;

/** Interface of Task Reactor input Queue for basic management of the work items */
public interface WorkItemQueue {

  /**
   * Fetch items currently not processed items and technical items used for other item canceling
   *
   * @return Subset of items from the queue
   */
  List<QueueItem> fetchNotProcessedAndCancelingItems();

  /**
   * Pushes singular work item to the queue.
   *
   * @param workItem Item scheduled to be processed by Task Reactor.
   */
  default void push(WorkItem workItem) {
    push(List.of(workItem));
  }

  /**
   * Pushes group of work items to the queue.
   *
   * @param workItems User defined items scheduled to be processed by Task Reactor.
   */
  void push(List<WorkItem> workItems);

  /**
   * Pushes cancel ongoing ingestion's item to work item queue.
   *
   * @param id Identifier of the work item to be cancelled.
   */
  default void cancelOngoingExecution(String id) {
    cancelOngoingExecutions(List.of(id));
  }

  /**
   * Pushes cancel ongoing ingestion's items to work item queue.
   *
   * @param ids Identifiers of the work items to be cancelled.
   */
  void cancelOngoingExecutions(List<String> ids);

  /**
   * Remove items from work item queue with given id.
   *
   * @param id Identifier of the object to be removed.
   */
  default void delete(String id) {
    delete(List.of(id));
  }

  /**
   * Remove items from work item queue with given ids.
   *
   * @param ids Identifiers of the object to be removed.
   */
  void delete(List<String> ids);

  /**
   * Remove items from work item queue which were created for a give resource id and before the
   * given timestamp.
   *
   * <p><i>Note: currently this method is considered temporary, it might be deleted in the future.
   * </i>
   *
   * @param resourceId resource id
   * @param timestamp limiting timestamp
   */
  void deleteBefore(String resourceId, Timestamp timestamp);

  /**
   * Gets a new instance of the default queue implementation.
   *
   * <p>Default implementation of the queue uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkItemQueue DefaultWorkItemQueue}, created
   *       for the {@code <schema>.QUEUE_TABLE} table.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new queue instance
   */
  static WorkItemQueue getInstance(Session session, Identifier schema) {
    return new DefaultWorkItemQueue(session, schema);
  }
}
