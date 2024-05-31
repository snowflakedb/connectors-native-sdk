/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for running a scheduler iteration. A new instance of the handler must be created using
 * {@link #builder(Session) the builder}.
 */
public class RunSchedulerIterationHandler {

  /**
   * Error type for the connector configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "SCHEDULER_ERROR";

  private final Scheduler scheduler;
  private final ConnectorErrorHelper errorHelper;

  RunSchedulerIterationHandler(Scheduler scheduler, ConnectorErrorHelper errorHelper) {
    this.scheduler = scheduler;
    this.errorHelper = errorHelper;
  }

  /**
   * Default handler method for the {@code PUBLIC.RUN_SCHEDULER_ITERATION} procedure.
   *
   * @param session Snowpark session object
   * @return a variant representing the {@link ConnectorResponse} returned by {@link #runIteration()
   *     runIteration}
   */
  public static Variant runIteration(Session session) {
    return builder(session).build().runIteration().toVariant();
  }

  /**
   * Returns a new instance of {@link RunSchedulerIterationHandlerBuilder}.
   *
   * @param session Snowpark session object
   * @return a new builder instance
   */
  public static RunSchedulerIterationHandlerBuilder builder(Session session) {
    return new RunSchedulerIterationHandlerBuilder(session);
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * <p>The handler logic consists of running a new scheduler iteration via the {@link
   * Scheduler#runIteration() runIteration} method.
   *
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse runIteration() {
    return errorHelper.withExceptionLogging(
        () -> {
          scheduler.runIteration();
          return ConnectorResponse.success();
        });
  }
}
