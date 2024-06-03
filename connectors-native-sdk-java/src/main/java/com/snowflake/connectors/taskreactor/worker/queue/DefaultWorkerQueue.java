/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import static com.snowflake.connectors.taskreactor.ComponentNames.workerQueueTable;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.SaveMode;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation of {@link WorkerQueue}. */
class DefaultWorkerQueue implements WorkerQueue {

  private static final Logger logger = LoggerFactory.getLogger(DefaultWorkerQueue.class);
  private static final StructType STRUCT_TYPE =
      StructType.create(
          new StructField("id", DataTypes.StringType),
          new StructField("resource_id", DataTypes.StringType),
          new StructField("worker_payload", DataTypes.VariantType));
  private final Session session;
  private final Identifier instanceSchema;

  DefaultWorkerQueue(Session session, Identifier instanceSchema) {
    this.session = session;
    this.instanceSchema = instanceSchema;
  }

  @Override
  public void push(QueueItem queueItem, WorkerId workerId) {
    logger.debug(
        "Pushing item {} for resource {} to worker {} queue.",
        queueItem.id,
        queueItem.resourceId,
        workerId.value());

    ObjectName queueName = ObjectName.from(instanceSchema, workerQueueTable(workerId));
    Row[] data = {Row.create(queueItem.id, queueItem.resourceId, queueItem.workerPayload)};
    session
        .createDataFrame(data, STRUCT_TYPE)
        .write()
        .mode(SaveMode.Append)
        .saveAsTable(queueName.getEscapedName());
  }

  @Override
  public WorkItem fetch(WorkerId workerId) {
    logger.debug("Fetching item from worker {} queue.", workerId.value());

    ObjectName queueName = ObjectName.from(instanceSchema, workerQueueTable(workerId));
    Row[] rows =
        session
            .table(queueName.getEscapedName())
            .select("id", "resource_id", "worker_payload")
            .collect();
    if (rows.length != 1) {
      logger.debug(
          "Failed to fetch item from worker {} queue. Multiple queue items detected.",
          workerId.value());

      throw new IllegalStateException(
          String.format("Invalid work items number (%d) on the queue", rows.length));
    }
    Row row = rows[0];
    return new WorkItem(row.getString(0), row.getString(1), row.getVariant(2));
  }

  @Override
  public void delete(WorkerId workerId) {
    logger.debug("Deleting item from worker {} queue.", workerId.value());

    ObjectName queueName = ObjectName.from(instanceSchema, workerQueueTable(workerId));
    session.table(queueName.getEscapedName()).delete();
  }
}
