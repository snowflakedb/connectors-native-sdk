/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link WorkerQueue}. */
public class InMemoryWorkerQueue implements WorkerQueue {

  private final Map<WorkerId, WorkItem> store = new HashMap<>();

  @Override
  public void push(QueueItem queueItem, WorkerId workerId) {
    store.put(workerId, new WorkItem(queueItem.id, queueItem.resourceId, queueItem.workerPayload));
  }

  @Override
  public WorkItem fetch(WorkerId workerId) {
    return store.get(workerId);
  }

  @Override
  public void delete(WorkerId workerId) {
    store.remove(workerId);
  }

  /**
   * Returns the map backing this queue.
   *
   * @return map backing this queue
   */
  public Map<WorkerId, WorkItem> store() {
    return store;
  }

  /** Clears this queue. */
  public void clear() {
    store.clear();
  }
}
