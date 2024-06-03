/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.troubleshooting;

import static java.lang.String.format;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A service providing troubleshooting data about a Task Reactor instance. */
class TaskReactorInstanceTroubleshootingDataService {

  private final Session session;
  private final Identifier instanceName;
  private final String categoryPrefix;

  TaskReactorInstanceTroubleshootingDataService(Session session, Identifier instanceName) {
    this.session = session;
    this.instanceName = instanceName;
    this.categoryPrefix = format("task_reactor:%s:", instanceName.toSqlString());
  }

  DataFrame getTroubleshootingData(long fromTimestamp, long toTimestamp) {
    return getConfig()
        .union(getDispatcherQueueCount())
        .union(getWorkerRegistry())
        .union(getWorkerQueueCount())
        .union(getTasksInfo())
        .union(getWorkerStatus(fromTimestamp, toTimestamp));
  }

  private DataFrame getConfig() {
    return session.sql(
        format(
            "select '%sconfig' as category, TO_VARIANT(OBJECT_CONSTRUCT(*)) as data FROM %s.config",
            categoryPrefix, instanceName.toSqlString()));
  }

  private DataFrame getDispatcherQueueCount() {
    return session.sql(
        format(
            "select '%sdispatcher_queue_count', count(*) from %s.queue",
            categoryPrefix, instanceName.toSqlString()));
  }

  private DataFrame getWorkerRegistry() {
    return session.sql(
        format(
            "select '%sworker_registry' as category, TO_VARIANT(OBJECT_CONSTRUCT(*)) FROM"
                + " %s.worker_registry",
            categoryPrefix, instanceName.toSqlString()));
  }

  private DataFrame getWorkerStatus(long from, long to) {
    return session.sql(
        format(
            "select '%sworker_status' as category, TO_VARIANT(OBJECT_CONSTRUCT(*)) FROM"
                + " %s.worker_status WHERE timestamp BETWEEN   to_timestamp_ntz(%s) AND"
                + " to_timestamp_ntz(%s)",
            categoryPrefix, instanceName.toSqlString(), from, to));
  }

  private DataFrame getWorkerQueueCount() {
    List<Long> workerIds = selectWorkerIds();
    String singleQueueSelect =
        " 'worker_%s_queue_count', (SELECT count(*) from %s.worker_queue_%s) ";

    String queryPrefix =
        format(
            "select '%sworker_queues_count' as category, TO_VARIANT(OBJECT_CONSTRUCT(",
            categoryPrefix);
    String query =
        workerIds.stream()
            .map(
                workerId ->
                    format(singleQueueSelect, workerId, instanceName.toSqlString(), workerId))
            .collect(Collectors.joining(",", queryPrefix, "))"));

    return session.sql(query);
  }

  private List<Long> selectWorkerIds() {
    Row[] result =
        session
            .sql(
                format(
                    "select * from %s.worker_registry where status in ('ACTIVE',"
                        + " 'UP_FOR_DELETION')",
                    instanceName.toSqlString()))
            .select("worker_id")
            .collect();
    return Arrays.stream(result).map(row -> row.getLong(0)).collect(Collectors.toList());
  }

  private DataFrame getTasksInfo() {
    String queryId =
        executeAndGetLastQueryId(format("show tasks in schema %s", instanceName.toSqlString()));
    return session.sql(
        format(
            "SELECT '%stask', TO_VARIANT(OBJECT_CONSTRUCT(*)) FROM TABLE(RESULT_SCAN('%s'))",
            categoryPrefix, queryId));
  }

  private String executeAndGetLastQueryId(String query) {
    session.sql(query).collect();
    return session.sql("SELECT LAST_QUERY_ID()").collect()[0].getString(0);
  }
}
