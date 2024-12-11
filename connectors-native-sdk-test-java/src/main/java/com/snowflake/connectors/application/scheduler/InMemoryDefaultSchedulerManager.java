/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskRepository;

/** InMemory implementation of {@link SchedulerManager}. */
public class InMemoryDefaultSchedulerManager extends DefaultSchedulerManager {

  /**
   * Creates a new {@link InMemoryDefaultSchedulerManager}.
   *
   * @param connectorConfigurationService connector configuration service
   * @param taskRepository task repository
   * @param taskLister task lister
   */
  public InMemoryDefaultSchedulerManager(
      ConnectorConfigurationService connectorConfigurationService,
      TaskRepository taskRepository,
      TaskLister taskLister) {
    super(connectorConfigurationService, taskRepository, taskLister);
  }
}
