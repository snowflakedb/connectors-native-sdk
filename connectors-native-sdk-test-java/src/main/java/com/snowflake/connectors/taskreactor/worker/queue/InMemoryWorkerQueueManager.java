/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.util.HashSet;
import java.util.Set;

/** In memory implementation of {@link WorkerQueueManager}. */
public class InMemoryWorkerQueueManager implements WorkerQueueManager {

  private final Set<WorkerId> workerQueues = new HashSet<>();

  @Override
  public void createWorkerQueueIfNotExist(WorkerId workerId) {
    workerQueues.add(workerId);
  }

  @Override
  public void dropWorkerQueue(WorkerId workerId) {
    workerQueues.remove(workerId);
  }

  /**
   * Returns the set backing this manager.
   *
   * @return set backing this manager
   */
  public Set<WorkerId> workerQueues() {
    return workerQueues;
  }

  /** Clears the set of queues in this manager. */
  public void clear() {
    workerQueues.clear();
  }
}
