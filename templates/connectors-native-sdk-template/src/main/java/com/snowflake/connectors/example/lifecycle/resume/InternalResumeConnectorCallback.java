/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.lifecycle.resume;

import static com.snowflake.connectors.example.ConnectorObjects.SCHEDULER_TASK;
import static com.snowflake.connectors.example.ConnectorObjects.STATE_SCHEMA;

import com.snowflake.connectors.application.lifecycle.resume.ResumeConnectorCallback;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskRepository;

/**
 * Custom implementation of {@link ResumeConnectorCallback}, used by the {@link
 * ResumeConnectorCustomHandler}, providing resumption of the scheduler system.
 */
public class InternalResumeConnectorCallback implements ResumeConnectorCallback {

  private static final String ERROR_CODE = "RESUME_CONNECTOR_FAILED";
  private static final String ERROR_MSG = "Unable to resume all connector tasks";

  private final TaskRepository taskRepository;

  public InternalResumeConnectorCallback(TaskRepository taskRepository) {
    this.taskRepository = taskRepository;
  }

  @Override
  public ConnectorResponse execute() {
    var schedulerTask = ObjectName.from(STATE_SCHEMA, SCHEDULER_TASK);

    try {
      taskRepository.fetch(schedulerTask).resume();
    } catch (Exception e) {
      return ConnectorResponse.error(ERROR_CODE, ERROR_MSG);
    }

    return ConnectorResponse.success();
  }
}
