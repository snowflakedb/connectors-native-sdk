/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Session;

/** Manager responsible for creating queues for given workers */
public interface WorkerQueueManager {

  /**
   * Creates queue table and snowflake stream on this table if objects do not exist.
   *
   * @param workerId worker identifier
   */
  void createWorkerQueueIfNotExist(WorkerId workerId);

  /**
   * Removes snowflake table and stream for provided workerId.
   *
   * @param workerId worker identifier
   */
  void dropWorkerQueue(WorkerId workerId);

  /**
   * Returns a new instance of the default manager implementation.
   *
   * <p>Default implementation of the manager uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkerQueueManager DefaultWorkerQueueManager}.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new manager instance
   */
  static WorkerQueueManager getInstance(Session session, Identifier schema) {
    return new DefaultWorkerQueueManager(session, schema);
  }
}
