/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/** Handler for creating a new scheduler. */
public class CreateSchedulerHandler {

  /**
   * Error type for the connector configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "CREATE_SCHEDULER_ERROR";

  private final ConnectorErrorHelper errorHelper;
  private final SchedulerManager schedulerManager;

  /**
   * Creates a new {@link CreateSchedulerHandler}.
   *
   * @param errorHelper connector error helper
   * @param schedulerManager scheduler manager
   */
  CreateSchedulerHandler(ConnectorErrorHelper errorHelper, SchedulerManager schedulerManager) {
    this.errorHelper = errorHelper;
    this.schedulerManager = schedulerManager;
  }

  /**
   * Default handler method for the {@code PUBLIC.CREATE_SCHEDULER} procedure.
   *
   * <p>This method uses:
   *
   * <ul>
   *   <li>a default implementation of {@link SchedulerManager}
   *   <li>{@link ConnectorErrorHelper} built using {@link
   *       ConnectorErrorHelper#buildDefault(Session, String) buildDefault}
   * </ul>
   *
   * @param session Snowpark session object
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #createScheduler() createScheduler}
   */
  public static Variant createScheduler(Session session) {
    var schedulerHandler = CreateSchedulerHandler.builder(session).build();
    return schedulerHandler.createScheduler().toVariant();
  }

  /**
   * Returns a new instance of {@link CreateSchedulerHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static CreateSchedulerHandlerBuilder builder(Session session) {
    return new CreateSchedulerHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of creating a scheduler via the {@link
   * SchedulerManager#createScheduler() createScheduler} method.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse createScheduler() {
    return errorHelper.withExceptionLoggingAndWrapping(schedulerManager::createScheduler);
  }
}
