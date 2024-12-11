/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationService;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.snowpark_java.Session;

/** Manager of {@link Scheduler} instances. */
public interface SchedulerManager {

  /**
   * Creates a new scheduler instance.
   *
   * @return a response with the code {@code OK} if the creation was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse createScheduler();

  /**
   * Resumes a scheduler task.
   *
   * @return a response with the code {@code OK} if the resuming was successful, otherwise a
   *     response with an error code and an error message
   */
  ConnectorResponse resumeScheduler();

  /**
   * Pauses a scheduler task.
   *
   * @return a response with the code {@code OK} if the pausing was successful, otherwise a response
   *     with an error code and an error message
   */
  ConnectorResponse pauseScheduler();

  /**
   * Checks if the scheduler task exists.
   *
   * @return {@code true} if the scheduler task exists, otherwise {@code false}
   */
  boolean schedulerExists();

  /**
   * Changes scheduler task schedule
   *
   * @param schedule CRON schedule to be changed to.
   */
  void alterSchedulerSchedule(String schedule);

  /**
   * Gets a new instance of the default scheduler manager implementation.
   *
   * <p>Default implementation of the scheduler manager uses:
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
   * @return a new scheduler manager instance
   */
  static SchedulerManager getInstance(Session session) {
    return new DefaultSchedulerManager(
        ConnectorConfigurationService.getInstance(session),
        TaskRepository.getInstance(session),
        TaskLister.getInstance(session));
  }
}
