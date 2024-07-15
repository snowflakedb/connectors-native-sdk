/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.status;

import static com.snowflake.connectors.taskreactor.ComponentNames.WORKER_STATUS_TABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository.ColumnNames.STATUS;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository.ColumnNames.TIMESTAMP;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository.ColumnNames.WORKER_ID;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.snowpark_java.Functions.col;
import static java.util.stream.Collectors.toSet;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.util.sql.TimestampUtil;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/** Default implementation of {@link WorkerStatusRepository} repository. */
class DefaultWorkerStatusRepository implements WorkerStatusRepository {

  private static final Logger LOG =
      TaskReactorLogger.getLogger(DefaultWorkerStatusRepository.class);

  private final Session session;
  private final ObjectName tableName;

  /**
   * Creates a new {@link DefaultWorkerStatusRepository}.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   */
  DefaultWorkerStatusRepository(Session session, Identifier instanceSchema) {
    this.session = session;
    this.tableName = ObjectName.from(instanceSchema, Identifier.from(WORKER_STATUS_TABLE));
  }

  @Override
  public Set<WorkerId> getAvailableWorkers() {
    LOG.debug("Fetching available workers.");

    String query =
        String.format(
            "SELECT DISTINCT WORKER_ID, FIRST_VALUE(STATUS) OVER (PARTITION BY WORKER_ID ORDER BY"
                + " TIMESTAMP DESC) AS LAST_STATUS FROM %s",
            tableName.getValue());
    Row[] workerIdRows =
        session
            .sql(query)
            .where(col("last_status").equal_to(lit(AVAILABLE.name())))
            .select(WORKER_ID)
            .collect();
    return Arrays.stream(workerIdRows).map(row -> new WorkerId(row.getInt(0))).collect(toSet());
  }

  @Override
  public WorkerStatus getStatusFor(WorkerId workerId) {
    LOG.debug("Fetching status for worker {}", workerId.value());

    return session
        .table(tableName.getValue())
        .where(col(WORKER_ID).equal_to(lit(workerId.value())))
        .sort(col(TIMESTAMP).desc())
        .limit(1)
        .select(col(STATUS))
        .first()
        .map(row -> row.getString(0))
        .map(WorkerStatus::valueOf)
        .orElseThrow(() -> new IllegalStateException("There is no status for worker " + workerId));
  }

  @Override
  public void updateStatusFor(WorkerId workerId, WorkerStatus status) {
    LOG.debug("Updating worker {} status to {}", workerId.value(), status);

    session
        .sql(
            String.format(
                "INSERT INTO %s (WORKER_ID, STATUS) VALUES (%s, %s)",
                tableName.getValue(), workerId.value(), asVarchar(status.name())))
        .toLocalIterator();
  }

  @Override
  public void updateStatusesFor(WorkerStatus status, List<WorkerId> workerIds) {
    LOG.debug("Updating workers {} to status {}.", workerIds, status);

    String values =
        workerIds.stream()
            .map(id -> String.format("(%s, %s)", id.value(), asVarchar(status.name())))
            .collect(Collectors.joining(","));

    session
        .sql(
            String.format(
                "INSERT INTO %s (WORKER_ID, STATUS) VALUES %s", tableName.getValue(), values))
        .toLocalIterator();
  }

  @Override
  public void removeStatusFor(WorkerId workerId) {
    LOG.debug("Removing status of worker {}.", workerId.value());

    session.table(tableName.getValue()).delete(col(WORKER_ID).equal_to(lit(workerId.value())));
  }

  @Override
  public Optional<Instant> getLastAvailable(WorkerId workerId) {
    return session
        .table(tableName.getValue())
        .where(
            col(WORKER_ID)
                .equal_to(lit(workerId.value()))
                .and(col(STATUS).equal_to(lit(AVAILABLE.name()))))
        .sort(col(TIMESTAMP).desc())
        .limit(1)
        .select(col(TIMESTAMP))
        .first()
        .map(row -> row.getTimestamp(0))
        .map(TimestampUtil::toInstant);
  }
}
