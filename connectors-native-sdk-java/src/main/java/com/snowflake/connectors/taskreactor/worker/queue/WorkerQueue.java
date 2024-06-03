/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Session;

/** Implementation of Task Reactor Worker Queue */
public interface WorkerQueue {

  /**
   * Pushes provided item to the queue.
   *
   * @param queueItem Item to be pushed.
   * @param workerId Identifier of a Worker Queue.
   */
  void push(QueueItem queueItem, WorkerId workerId);

  /**
   * Fetches exactly one item from a Worker Queue. If there are multiple items throws exception due
   * to invalid.
   *
   * @param workerId Identifier of a Worker Queue.
   * @return Item selected from the queue.
   */
  WorkItem fetch(WorkerId workerId);

  /**
   * Delete item in given Worker Queue.
   *
   * @param workerId Identifier of a Worker Queue.
   */
  void delete(WorkerId workerId);

  /**
   * Returns a new instance of the default queue implementation.
   *
   * <p>Default implementation of the queue uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkerQueue DefaultWorkerQueue}, created for
   *       the {@code <schema>.QUEUE_<worker-id>} table.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new queue instance
   */
  static WorkerQueue getInstance(Session session, Identifier schema) {
    return new DefaultWorkerQueue(session, schema);
  }
}
