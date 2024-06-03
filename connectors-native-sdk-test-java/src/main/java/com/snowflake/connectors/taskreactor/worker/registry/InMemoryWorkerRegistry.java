/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** In memory implementation of {@link WorkerRegistry}. */
public class InMemoryWorkerRegistry implements WorkerRegistry {

  private final Map<WorkerId, WorkerLifecycleStatus> store = new HashMap<>();

  @Override
  public void insertWorkers(int workersToInsert) {
    int startingValue =
        store.keySet().stream()
            .max(Comparator.comparing(WorkerId::value))
            .map(WorkerId::value)
            .orElse(0);

    IntStream.range(startingValue + 1, startingValue + 1 + workersToInsert)
        .mapToObj(WorkerId::new)
        .forEach(id -> store.put(id, WorkerLifecycleStatus.REQUESTED));
  }

  @Override
  public void setWorkersStatus(WorkerLifecycleStatus status, List<WorkerId> workerIds) {
    workerIds.stream().filter(store::containsKey).forEach(id -> store.put(id, status));
  }

  @Override
  public long updateWorkersStatus(
      WorkerLifecycleStatus newStatus,
      WorkerLifecycleStatus currentStatus,
      Collection<WorkerId> workerIds) {
    return workerIds.stream()
        .filter(store::containsKey)
        .filter(id -> store.get(id) == currentStatus)
        .map(id -> store.put(id, newStatus))
        .count();
  }

  @Override
  public List<WorkerId> getWorkerIds(WorkerLifecycleStatus... statuses) {
    return store.entrySet().stream()
        .filter(entry -> Arrays.stream(statuses).anyMatch(status -> status == entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  @Override
  public int getWorkerCountWithStatuses(WorkerLifecycleStatus... statuses) {
    return getWorkerIds(statuses).size();
  }

  /**
   * Returns a map backing this registry.
   *
   * @return map backing this registry
   */
  public Map<WorkerId, WorkerLifecycleStatus> store() {
    return store;
  }

  /** Clears this registry. */
  public void clear() {
    store.clear();
  }
}
