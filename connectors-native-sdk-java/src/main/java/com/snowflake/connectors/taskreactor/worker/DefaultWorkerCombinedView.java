/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static com.snowflake.connectors.taskreactor.ComponentNames.WORKER_COMBINED_VIEW;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.snowpark_java.Functions.col;
import static java.util.stream.Collectors.joining;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;

/** Default implementation of {@link WorkerCombinedView} view. */
public class DefaultWorkerCombinedView implements WorkerCombinedView {

  private static final Logger LOG = TaskReactorLogger.getLogger(DefaultWorkerCombinedView.class);

  private final Session session;
  private final Identifier instanceSchema;
  private final ObjectName viewName;

  DefaultWorkerCombinedView(Session session, Identifier instanceSchema) {
    this.session = session;
    this.instanceSchema = instanceSchema;
    this.viewName = ObjectName.from(instanceSchema, Identifier.from(WORKER_COMBINED_VIEW));
  }

  @Override
  public List<WorkerId> getWorkersExecuting(List<String> ids) {
    LOG.debug("Fetching workers executing with ids: {}", ids);

    Row[] rows =
        session
            .table(viewName.getValue())
            .select("WORKER_ID")
            .where(col("ID").in(ids.toArray()))
            .collect();
    return Arrays.stream(rows).map(row -> new WorkerId(row.getInt(0))).collect(Collectors.toList());
  }

  @Override
  public Stream<WorkerId> getWorkersExecuting(String resourceId) {
    LOG.debug("Fetching workers executing resource: {}", resourceId);

    Row[] rows =
        session
            .table(viewName.getValue())
            .where(col("RESOURCE_ID").equal_to(lit(resourceId)))
            .select("WORKER_ID")
            .collect();
    return Arrays.stream(rows).map(row -> new WorkerId(row.getInt(0)));
  }

  @Override
  public void recreate(List<WorkerId> workerIds) {
    if (workerIds.isEmpty()) {
      LOG.debug("Creating empty worker combined view.");
      createEmptyView();
    } else {
      LOG.debug("Creating worker combined view for workers {}.", workerIds);
      createViewFor(workerIds);
    }
  }

  private void createEmptyView() {
    String emptyViewSql =
        String.format(
            "CREATE OR REPLACE VIEW %s AS SELECT 1 AS WORKER_ID, '' AS WORKER_QUEUE, '' AS ID,"
                + " '' AS RESOURCE_ID, NULL AS WORKER_PAYLOAD FROM (VALUES(1)) WHERE WORKER_ID = 2",
            viewName.getValue());
    session.sql(emptyViewSql).toLocalIterator();
  }

  private void createViewFor(List<WorkerId> workerIds) {
    String joiningQuery =
        workerIds.stream()
            .map(this::generateSelectFromQueueForWorker)
            .collect(joining(" UNION ALL "));
    session
        .sql(String.format("CREATE OR REPLACE VIEW %s AS %s", viewName.getValue(), joiningQuery))
        .toLocalIterator();
  }

  private String generateSelectFromQueueForWorker(WorkerId workerId) {
    String workerQueueTable = ComponentNames.workerQueueTable(workerId).getValue();

    return String.format(
        "SELECT %d AS WORKER_ID, %s AS WORKER_QUEUE, * FROM %s.%s",
        workerId.value(), asVarchar(workerQueueTable), instanceSchema.getValue(), workerQueueTable);
  }
}
