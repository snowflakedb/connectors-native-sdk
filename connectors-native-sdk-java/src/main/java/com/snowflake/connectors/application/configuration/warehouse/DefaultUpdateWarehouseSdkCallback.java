/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.configuration.warehouse;

import static com.snowflake.connectors.application.scheduler.Scheduler.SCHEDULER_TASK;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.common.task.UpdateTaskReactorTasks;
import com.snowflake.connectors.taskreactor.TaskReactorExistenceVerifier;
import com.snowflake.snowpark_java.Session;

/** Default implementation of {@link UpdateWarehouseSdkCallback}. */
class DefaultUpdateWarehouseSdkCallback implements UpdateWarehouseSdkCallback {

  private final TaskReactorExistenceVerifier taskReactorVerifier;
  private final TaskRepository taskRepository;
  private final UpdateTaskReactorTasks updateTaskReactorTasks;

  DefaultUpdateWarehouseSdkCallback(Session session) {
    this.taskReactorVerifier = TaskReactorExistenceVerifier.getInstance(session);
    this.taskRepository = TaskRepository.getInstance(session);
    this.updateTaskReactorTasks = UpdateTaskReactorTasks.getInstance(session);
  }

  @Override
  public ConnectorResponse execute(Identifier warehouse) {
    updateSchedulerTask(warehouse);
    updateTaskReactorTasks(warehouse);

    return ConnectorResponse.success();
  }

  private void updateSchedulerTask(Identifier warehouse) {
    var schedulerTask = taskRepository.fetch(SCHEDULER_TASK);
    schedulerTask.alterWarehouseIfExists(warehouse.toSqlString());
  }

  private void updateTaskReactorTasks(Identifier warehouse) {
    if (taskReactorVerifier.isTaskReactorConfigured()) {
      updateTaskReactorTasks.update(warehouse);
    }
  }
}
