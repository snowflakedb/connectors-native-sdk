/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.worker.registry.WorkerOrchestrator;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;

/** Handler for the Task Reactor worker number update. */
public class SetWorkersNumberHandler {

  private static final Logger LOG = TaskReactorLogger.getLogger(SetWorkersNumberHandler.class);

  /**
   * Default handler method for the {@code TASK_REACTOR.SET_WORKERS_NUMBER} procedure.
   *
   * <p>Sets the desired number of workers in the for a Task Reactor instance. Statuses of each
   * worker can be queried from the {@code WORKER_REGISTRY} table in the Task Reactor schema.
   *
   * <p>Note: This procedure only manages the values in the {@code WORKER_REGISTRY} table. It does
   * not create, nor delete, worker tables, streams or tasks. This is a job of a dispatcher
   * procedure. In the next run the dispatcher will create requested workers and remove workers
   * marked for deletion, updating their status in the {@code WORKER_REGISTRY} table.
   *
   * @param session Snowpark session object
   * @param workersNumber desired number of workers, greater or equal to 0
   * @param instanceSchema task reactor instance name
   * @return message about how many workers were scheduled for adding or deletion
   */
  public static String setWorkersNumber(Session session, int workersNumber, String instanceSchema) {
    LOG.info("Setting workers number for instance {} to {}.", instanceSchema, workersNumber);
    WorkerOrchestrator workerOrchestrator =
        WorkerOrchestrator.from(session, Identifier.from(instanceSchema));

    String result = workerOrchestrator.setWorkersNumber(workersNumber);
    LOG.info("{} (instance: {})", result, instanceSchema);
    return result;
  }
}
