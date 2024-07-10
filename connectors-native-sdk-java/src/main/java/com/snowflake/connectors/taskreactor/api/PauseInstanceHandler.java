/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.taskreactor.lifecycle.PauseTaskReactorService;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the Task Reactor instance pausing process. A new instance of the handler must be
 * created using {@link PauseInstanceHandlerBuilder the builder}.
 */
public class PauseInstanceHandler {

  /**
   * Error type for the connection configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "PAUSE_TASK_REACTOR_INSTANCE_FAILED";

  private final ConnectorErrorHelper errorHelper;
  private final PauseTaskReactorService pauseTaskReactorService;

  PauseInstanceHandler(
      ConnectorErrorHelper errorHelper, PauseTaskReactorService pauseTaskReactorService) {
    this.pauseTaskReactorService = pauseTaskReactorService;
    this.errorHelper = errorHelper;
  }

  /**
   * Default handler method for the {@code TASK_REACTOR.PAUSE_INSTANCE} procedure.
   *
   * <p>It inserts the {@code PAUSE_INSTANCE} command into the Task Reactor command queue.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance name
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #pauseInstance(Identifier) pauseInstance}
   */
  public static Variant pauseInstance(Session session, String instanceSchema) {
    Identifier identifier = Identifier.from(instanceSchema);
    return new PauseInstanceHandlerBuilder(session).build().pauseInstance(identifier).toVariant();
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * @param instanceSchema Task Reactor instance name
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse pauseInstance(Identifier instanceSchema) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> {
          pauseTaskReactorService.pauseInstance(instanceSchema);
          return ConnectorResponse.success();
        });
  }
}
