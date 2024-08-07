/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.registry;

import static com.snowflake.connectors.taskreactor.ComponentNames.WORKER_REGISTRY_TABLE;
import static com.snowflake.connectors.taskreactor.worker.registry.WorkerLifecycleStatus.REQUESTED;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.sysdate;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;

/** Default implementation of {@link WorkerRegistry} registry. */
class DefaultWorkerRegistry implements WorkerRegistry {

  private static final Logger LOG = TaskReactorLogger.getLogger(DefaultWorkerRegistry.class);

  private final Session session;
  private final ObjectName registryName;

  DefaultWorkerRegistry(Session session, Identifier instanceSchema) {
    this.session = session;
    this.registryName = ObjectName.from(instanceSchema, Identifier.from(WORKER_REGISTRY_TABLE));
  }

  @Override
  public void insertWorkers(int workersToInsert) {
    LOG.debug("Requesting creation of {} workers.", workersToInsert);

    String insertParams =
        IntStream.range(0, workersToInsert)
            .mapToObj($ -> String.format("(%s)", asVarchar(REQUESTED.name())))
            .collect(Collectors.joining(", "));
    session
        .sql(
            String.format(
                "INSERT INTO %s (STATUS) VALUES %s", registryName.getValue(), insertParams))
        .toLocalIterator();
  }

  @Override
  public void setWorkersStatus(WorkerLifecycleStatus status, List<WorkerId> workerIds) {
    LOG.debug("Setting status {} to workers {}.", status, workerIds);

    Object[] workerIdsArray = workerIds.stream().map(WorkerId::value).toArray();
    Map<String, Column> updates =
        Map.of(
            "STATUS", lit(status.name()),
            "UPDATED_AT", sysdate());
    session
        .table(registryName.getValue())
        .updateColumn(updates, col("WORKER_ID").in(workerIdsArray));
  }

  @Override
  public long updateWorkersStatus(
      WorkerLifecycleStatus newStatus,
      WorkerLifecycleStatus currentStatus,
      Collection<WorkerId> workerIds) {
    if (workerIds.isEmpty()) {
      return 0;
    }

    LOG.debug("Updating status {} to {} for {} workers.", newStatus, currentStatus, workerIds);
    Object[] workerIdsArray = workerIds.stream().map(WorkerId::value).toArray();
    Map<String, Column> updates =
        Map.of(
            "STATUS", lit(newStatus.name()),
            "UPDATED_AT", sysdate());
    Column condition =
        col("WORKER_ID").in(workerIdsArray).and(col("STATUS").equal_to(lit(currentStatus.name())));
    return session.table(registryName.getValue()).updateColumn(updates, condition).getRowsUpdated();
  }

  @Override
  public List<WorkerId> getWorkerIds(WorkerLifecycleStatus... statuses) {
    LOG.debug("Getting worker ids for statuses {}", Arrays.toString(statuses));

    Object[] statusNames = Arrays.stream(statuses).map(WorkerLifecycleStatus::name).toArray();
    return Arrays.stream(
            session
                .table(registryName.getValue())
                .where(col("STATUS").in(statusNames))
                .select("WORKER_ID")
                .collect())
        .map(row -> row.getInt(0))
        .map(WorkerId::new)
        .collect(toList());
  }

  @Override
  public int getWorkerCountWithStatuses(WorkerLifecycleStatus... statuses) {
    LOG.debug("Getting amount of workers in statuses {}", Arrays.toString(statuses));

    Object[] statusNames = Arrays.stream(statuses).map(WorkerLifecycleStatus::name).toArray();
    return (int)
        session.table(registryName.getValue()).where(col("STATUS").in(statusNames)).count();
  }
}
