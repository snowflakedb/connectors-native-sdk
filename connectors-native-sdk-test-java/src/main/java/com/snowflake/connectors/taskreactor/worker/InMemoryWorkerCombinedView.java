/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** In memory implementation of {@link WorkerCombinedView}. */
public class InMemoryWorkerCombinedView implements WorkerCombinedView {

  private final List<WorkerId> executingWorkers = new ArrayList<>();

  @Override
  public List<WorkerId> getWorkersExecuting(List<String> ids) {
    return executingWorkers;
  }

  @Override
  public Stream<WorkerId> getWorkersExecuting(String resourceId) {
    return executingWorkers.stream();
  }

  @Override
  public void recreate(List<WorkerId> workerIds) {}

  /**
   * Adds new executing workers.
   *
   * @param workerIds worker ids
   */
  public void addExecutingWorkers(List<WorkerId> workerIds) {
    executingWorkers.addAll(workerIds);
  }

  /** Clears the list of executing workers. */
  public void clear() {
    executingWorkers.clear();
  }
}
