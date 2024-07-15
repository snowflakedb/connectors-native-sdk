/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskRepository;

/** InMemory implementation of {@link SchedulerCreator}. */
public class InMemoryDefaultSchedulerCreator extends DefaultSchedulerCreator {

  public InMemoryDefaultSchedulerCreator(
      ConnectorConfigurationService connectorConfigurationService,
      TaskRepository taskRepository,
      TaskLister taskLister) {
    super(connectorConfigurationService, taskRepository, taskLister);
  }
}
