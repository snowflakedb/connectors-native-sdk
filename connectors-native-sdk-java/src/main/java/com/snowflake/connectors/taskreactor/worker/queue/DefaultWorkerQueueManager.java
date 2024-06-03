/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation of {@link WorkerQueueManager}. */
class DefaultWorkerQueueManager implements WorkerQueueManager {

  private static final Logger logger = LoggerFactory.getLogger(DefaultWorkerQueueManager.class);

  private final Session session;
  private final Identifier instanceSchema;

  DefaultWorkerQueueManager(Session session, Identifier instanceSchema) {
    this.session = session;
    this.instanceSchema = instanceSchema;
  }

  /**
   * Creates a worker queue if it does not already exist.
   *
   * @param workerId worker id
   */
  public void createWorkerQueueIfNotExist(WorkerId workerId) {
    logger.debug(
        "Create queue table and stream for worker {} in schema {}.",
        workerId,
        instanceSchema.toSqlString());
    ObjectName tableName =
        ObjectName.from(instanceSchema, ComponentNames.workerQueueTable(workerId));
    session
        .sql(
            String.format(
                "CREATE TABLE IF NOT EXISTS %s (ID STRING, RESOURCE_ID"
                    + " STRING, WORKER_PAYLOAD VARIANT)",
                tableName.getEscapedName()))
        .toLocalIterator();
  }

  /**
   * Drops a worker queue.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if the
   * queue does not exist.
   *
   * @param workerId worker id SnowflakeSql
   */
  public void dropWorkerQueue(WorkerId workerId) {
    logger.debug(
        "Dropping queue table and stream for worker {} in schema {}.",
        workerId,
        instanceSchema.toSqlString());
    ObjectName tableName =
        ObjectName.from(instanceSchema, ComponentNames.workerQueueTable(workerId));
    session.sql(String.format("DROP TABLE %s", tableName.getEscapedName())).toLocalIterator();
  }
}
