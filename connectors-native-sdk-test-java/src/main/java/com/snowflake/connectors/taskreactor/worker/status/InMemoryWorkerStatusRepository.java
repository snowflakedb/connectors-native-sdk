/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;

import com.snowflake.connectors.taskreactor.worker.WorkerId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** In memory implementation of {@link WorkerStatusRepository}. */
public class InMemoryWorkerStatusRepository implements WorkerStatusRepository {

  private final List<WorkerStatusEntry> repository = new ArrayList<>();

  @Override
  public Set<WorkerId> getAvailableWorkers() {
    return store().entrySet().stream()
        .filter(entry -> entry.getValue() == WorkerStatus.AVAILABLE)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Override
  public WorkerStatus getStatusFor(WorkerId workerId) {
    return store().get(workerId);
  }

  @Override
  public void updateStatusFor(WorkerId workerId, WorkerStatus status) {
    repository.add(new WorkerStatusEntry(status, workerId));
  }

  @Override
  public void updateStatusesFor(WorkerStatus status, List<WorkerId> workerIds) {
    workerIds.forEach(id -> repository.add(new WorkerStatusEntry(status, id)));
  }

  @Override
  public void removeStatusFor(WorkerId workerId) {
    var toRemove =
        repository.stream()
            .filter(workerStatus -> workerStatus.workerId.equals(workerId))
            .collect(Collectors.toList());
    repository.removeAll(toRemove);
  }

  @Override
  public Optional<Instant> getLastAvailable(WorkerId workerId) {
    return repository.stream()
        .filter(workerStatus -> workerStatus.workerId.equals(workerId))
        .filter(workerStatus -> workerStatus.status == WorkerStatus.AVAILABLE)
        .max(Comparator.comparing(WorkerStatusEntry::getTimestamp))
        .map(WorkerStatusEntry::getTimestamp);
  }

  @Override
  public Map<String, Integer> getWorkersNumberForEachStatus() {
    return store().values().stream().collect(groupingBy(Enum::name, summingInt(x -> 1)));
  }

  /**
   * Returns a map containing worker statuses assigned to worker ids.
   *
   * @return map containing worker statuses assigned to worker ids
   */
  public Map<WorkerId, WorkerStatus> store() {
    Map<WorkerId, List<WorkerStatusEntry>> mapByWorkerId =
        repository.stream().collect(groupingBy(WorkerStatusEntry::getWorkerId));
    return mapByWorkerId.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .max(Comparator.comparing(WorkerStatusEntry::getTimestamp))
                        .map(WorkerStatusEntry::getStatus)
                        .get()));
  }

  /** Clears this repository. */
  public void clear() {
    repository.clear();
  }

  private static class WorkerStatusEntry {

    WorkerStatus status;
    WorkerId workerId;
    Instant timestamp;

    /**
     * Creates a new worker status entry.
     *
     * @param status worker status
     * @param workerId worker id
     */
    public WorkerStatusEntry(WorkerStatus status, WorkerId workerId) {
      this.status = status;
      this.workerId = workerId;
      this.timestamp = Instant.now();
    }

    /**
     * Returns the worker creation timestamp.
     *
     * @return worker creation timestamp
     */
    public Instant getTimestamp() {
      return timestamp;
    }

    /**
     * Returns the worker status.
     *
     * @return worker status
     */
    public WorkerStatus getStatus() {
      return status;
    }

    /**
     * Returns the worker id.
     *
     * @return worker id
     */
    public WorkerId getWorkerId() {
      return workerId;
    }
  }
}
