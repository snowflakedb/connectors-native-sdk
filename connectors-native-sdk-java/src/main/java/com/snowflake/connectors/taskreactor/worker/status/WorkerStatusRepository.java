/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Session;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface WorkerStatusRepository {

  /**
   * Returns the ids of all available workers.
   *
   * @return set of ids of all available workers
   */
  Set<WorkerId> getAvailableWorkers();

  /**
   * Returns the status of a worker with the specified id.
   *
   * @param workerId worker id
   * @return status of a worker with the specified id
   */
  WorkerStatus getStatusFor(WorkerId workerId);

  /**
   * Updates the status of a worker with the specified id.
   *
   * @param workerId worker id
   * @param status new worker status
   */
  void updateStatusFor(WorkerId workerId, WorkerStatus status);

  /**
   * Updates the status of workers with the specified ids.
   *
   * @param status new workers status
   * @param workerIds identifiers of workers
   */
  void updateStatusesFor(WorkerStatus status, List<WorkerId> workerIds);

  /**
   * Removes the status for a worker with the specified id.
   *
   * @param workerId worker id
   */
  void removeStatusFor(WorkerId workerId);

  /**
   * Returns the last timestamp when given worker was in available status
   *
   * @param workerId id of worker
   * @return timestamp column for the last record when a worker with given id was in AVAILABLE
   *     status
   */
  Optional<Instant> getLastAvailable(WorkerId workerId);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkerStatusRepository
   *       DefaultWorkerStatusRepository}.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new repository instance
   */
  static WorkerStatusRepository getInstance(Session session, Identifier schema) {
    return new DefaultWorkerStatusRepository(session, schema);
  }

  /** Column names of the worker_status table. */
  class ColumnNames {
    /** Representation of the worker_id column. */
    public static final String WORKER_ID = "worker_id";

    /** Representation of the status column. */
    public static final String STATUS = "status";

    /** Representation of the timestamp column. */
    public static final String TIMESTAMP = "timestamp";
  }
}
