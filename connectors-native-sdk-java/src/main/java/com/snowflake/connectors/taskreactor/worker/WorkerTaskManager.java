/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.config.TaskReactorConfig;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerTaskManager {

  private static final Logger logger = LoggerFactory.getLogger(WorkerTaskManager.class);

  private final Identifier instanceSchema;
  private final ConfigRepository configRepository;
  private final TaskRepository taskRepository;

  /**
   * Creates a new {@link WorkerTaskManager}.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   * @return new manager instance
   */
  public static WorkerTaskManager from(Session session, Identifier instanceSchema) {
    return new WorkerTaskManager(
        instanceSchema,
        ConfigRepository.getInstance(session, instanceSchema),
        TaskRepository.getInstance(session));
  }

  WorkerTaskManager(
      Identifier instanceSchema, ConfigRepository configRepository, TaskRepository taskRepository) {
    this.instanceSchema = instanceSchema;
    this.configRepository = configRepository;
    this.taskRepository = taskRepository;
  }

  /**
   * Creates a new worker task.
   *
   * @param workerId worker id
   */
  public void createWorkerTask(WorkerId workerId) {
    logger.debug("Creating new worker for id {}.", workerId);

    ObjectName taskName = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    TaskReactorConfig config = configRepository.getConfig();
    String workerProcedure = config.workerProcedure();
    String warehouse = config.warehouse();

    String procedure =
        String.format(
            "CALL %s(%d, '%s')", workerProcedure, workerId.value(), instanceSchema.getName());
    TaskProperties taskProperties =
        new TaskProperties.Builder(taskName, procedure, "1 MINUTE")
            .withWarehouse(warehouse)
            .build();
    TaskDefinition taskDefinition = new TaskDefinition(taskProperties);
    taskRepository.create(taskDefinition, false, true);
  }

  /**
   * Drops a worker task.
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if the
   * queue does not exist.
   *
   * @param workerId worker id
   */
  public void dropWorkerTask(WorkerId workerId) {
    logger.debug("Removing worker with id {}.", workerId);

    ObjectName workerTask = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    taskRepository.fetch(workerTask).drop();
  }

  /**
   * Alters a worker task warehouse
   *
   * <p>Throws {@link net.snowflake.client.jdbc.SnowflakeSQLException SnowflakeSQLException} if the
   * queue does not exist.
   *
   * @param workerId worker id
   * @param warehouseName new warehouse name
   */
  public void alterWarehouse(WorkerId workerId, String warehouseName) {
    logger.debug("Altering warehouse of worker with id {}.", workerId);

    var workerTaskInstance = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    var workerTask = taskRepository.fetch(workerTaskInstance);
    workerTask.alterWarehouse(warehouseName);
  }

  /**
   * Suspends a worker task if it exists.
   *
   * @param workerId worker id
   */
  public void suspendWorkerTaskIfExists(WorkerId workerId) {
    logger.debug("Suspending worker with id {}.", workerId);

    ObjectName workerTask = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    taskRepository.fetch(workerTask).suspendIfExists();
  }

  /**
   * Resumes a worker task if it exists.
   *
   * @param workerId worker id
   */
  public void resumeWorkerTask(WorkerId workerId) {
    logger.debug("Resuming worker with id {}.", workerId);

    ObjectName workerTask = ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId));
    taskRepository.fetch(workerTask).resume();
  }
}
