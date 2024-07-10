/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import static com.snowflake.connectors.taskreactor.ComponentNames.QUEUE_TABLE;
import static com.snowflake.connectors.taskreactor.ComponentNames.WORKER_COMBINED_VIEW;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.connectors.util.sql.SqlTools.asCommaSeparatedSqlList;
import static com.snowflake.snowpark_java.Functions.col;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.util.sql.MergeStatementValidator;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;

/** Default implementation of {@link WorkItemQueue}. */
public class DefaultWorkItemQueue implements WorkItemQueue {

  private static final Logger LOG = TaskReactorLogger.getLogger(DefaultWorkItemQueue.class);

  private final Session session;
  private final Identifier instanceSchema;
  private final ObjectName queueName;

  DefaultWorkItemQueue(Session session, Identifier instanceSchema) {
    this.session = session;
    this.instanceSchema = instanceSchema;
    this.queueName = ObjectName.from(instanceSchema, Identifier.from(QUEUE_TABLE));
  }

  @Override
  public List<QueueItem> fetchNotProcessedAndCancelingItems() {
    LOG.debug("Fetching currently not processed and canceling items.");

    Row[] rawRows =
        session
            .sql(
                String.format(
                    "SELECT ID, TIMESTAMP, RESOURCE_ID, DISPATCHER_OPTIONS, WORKER_PAYLOAD FROM"
                        + " %s queue WHERE NOT EXISTS (SELECT 1 FROM %s.%s queues WHERE"
                        + " queue.RESOURCE_ID = queues.RESOURCE_ID) OR"
                        + " queue.DISPATCHER_OPTIONS:cancelOngoingExecution = true",
                    queueName.getValue(), instanceSchema.getValue(), WORKER_COMBINED_VIEW))
            .select("ID", "TIMESTAMP", "RESOURCE_ID", "DISPATCHER_OPTIONS", "WORKER_PAYLOAD")
            .collect();
    return Arrays.stream(rawRows).map(QueueItem::fromRow).collect(toList());
  }

  @Override
  public void push(List<WorkItem> workItems) {
    if (workItems.isEmpty()) {
      LOG.debug("Skip pushing items to queue {}, due to lack of work items.", queueName.getValue());
      return;
    }

    MergeStatementValidator.validateRecordLimit(workItems);
    MergeStatementValidator.validateDuplicates(workItems, workItem -> workItem.id);

    var rows = workItems.stream().map(items -> "(?,?,?)").collect(joining(","));
    Connection connection = session.jdbcConnection();
    var query =
        String.format(
            "MERGE INTO %s AS target USING (SELECT id, resource_id, PARSE_JSON(worker_payload) AS"
                + " WORKER_PAYLOAD FROM (VALUES %s) AS V(resource_id, worker_payload)) AS source ON"
                + " ('1' = '1') WHEN NOT MATCHED THEN INSERT (\"ID\", \"RESOURCE_ID\","
                + " \"WORKER_PAYLOAD\", \"TIMESTAMP\") VALUES (source.id, source.resource_id,"
                + " source.worker_payload, SYSDATE()) ",
            queueName.getValue(), rows);

    try {
      PreparedStatement preparedStatement = connection.prepareStatement(query);
      int i = 1;
      for (WorkItem item : workItems) {
        preparedStatement.setString(i++, item.id);
        preparedStatement.setString(i++, item.resourceId);
        preparedStatement.setString(i++, item.payload.asString());
      }

      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOG.error(
          "Failed to push items to queue {}. Merge statement failed with exception:",
          queueName.getValue(),
          e);
      throw new WorkItemQueueException(e);
    }
  }

  @Override
  public void cancelOngoingExecutions(List<String> resourceIds) {
    LOG.debug("Pushing {} items to WorkItemQueue to cancel properties.", resourceIds.size());

    String parsedIds = asCommaSeparatedSqlList(resourceIds);

    session
        .sql(
            String.format(
                "INSERT INTO %s (RESOURCE_ID, DISPATCHER_OPTIONS)"
                    + " SELECT VALUE, PARSE_JSON('{\"cancelOngoingExecution\": true}') FROM"
                    + " TABLE(FLATTEN(PARSE_JSON('[%s]')))",
                queueName.getValue(), parsedIds))
        .toLocalIterator();
  }

  @Override
  public void delete(List<String> ids) {
    if (ids.isEmpty()) {
      LOG.debug("There are no work items to be removed from the WorkItemQueue.");
      return;
    }
    LOG.debug("Removing {} items from WorkItemQueue.", ids.size());

    String parsedIds = asCommaSeparatedSqlList(ids);
    session
        .sql(String.format("DELETE FROM %s WHERE ID IN (%s)", queueName.getValue(), parsedIds))
        .toLocalIterator();
  }

  @Override
  public void deleteBefore(String resourceId, Timestamp timestamp) {
    Column condition =
        col("RESOURCE_ID").equal_to(lit(resourceId)).and(col("TIMESTAMP").leq(lit(timestamp)));
    session.table(queueName.getValue()).delete(condition);
  }
}
