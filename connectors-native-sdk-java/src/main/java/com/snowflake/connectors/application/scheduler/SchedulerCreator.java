/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.Session;

/** Creator for instances of {@link Scheduler}. */
public interface SchedulerCreator {

  /**
   * Creates a new scheduler instance.
   *
   * @return a response with the code {@code OK} if the creation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse createScheduler();

  /**
   * Gets a new instance of the default scheduler creator implementation.
   *
   * <p>Default implementation of the creator uses:
   *
   * <ul>
   *   <li>a default implementation of {@link ConnectorConfigurationService}
   *   <li>a default implementation of {@link TaskRepository}
   *   <li>a default implementation of {@link TaskLister}
   * </ul>
   *
   * <p>The default scheduler creation process consists of:
   *
   * <ul>
   *   <li>creation of {@code STATE.SCHEDULER_TASK} task
   *   <li>granting of {@code MONITOR} task privilege to the {@code ADMIN} application role
   *   <li>resumption of the task
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new creator instance
   */
  static SchedulerCreator getInstance(Session session) {
    return new DefaultSchedulerCreator(
        ConnectorConfigurationService.getInstance(session),
        TaskRepository.getInstance(session),
        TaskLister.getInstance(session));
  }
}
