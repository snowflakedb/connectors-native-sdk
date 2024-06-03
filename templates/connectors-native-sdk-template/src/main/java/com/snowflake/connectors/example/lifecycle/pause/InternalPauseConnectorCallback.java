/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.pause;

import static com.snowflake.connectors.example.ConnectorObjects.SCHEDULER_TASK;
import static com.snowflake.connectors.example.ConnectorObjects.STATE_SCHEMA;

import com.snowflake.connectors.application.lifecycle.pause.PauseConnectorCallback;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskRepository;

/**
 * Custom implementation of {@link PauseConnectorCallback}, used by the {@link
 * PauseConnectorCustomHandler}, providing suspension of the scheduler system.
 */
public class InternalPauseConnectorCallback implements PauseConnectorCallback {

  private static final String ERROR_CODE = "PAUSE_CONNECTOR_FAILED";
  private static final String ERROR_MSG = "Unable to suspend all connector tasks";

  private final TaskRepository taskRepository;

  public InternalPauseConnectorCallback(TaskRepository taskRepository) {
    this.taskRepository = taskRepository;
  }

  @Override
  public ConnectorResponse execute() {
    var schedulerTask = ObjectName.from(STATE_SCHEMA, SCHEDULER_TASK);

    try {
      taskRepository.fetch(schedulerTask).suspend();
    } catch (Exception e) {
      return ConnectorResponse.error(ERROR_CODE, ERROR_MSG);
    }

    return ConnectorResponse.success();
  }
}
