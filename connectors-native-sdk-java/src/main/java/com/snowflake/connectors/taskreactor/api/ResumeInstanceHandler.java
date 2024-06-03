/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.api;

import com.snowflake.connectors.common.exception.helper.ConnectorErrorHelper;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.taskreactor.lifecycle.ResumeTaskReactorService;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.function.Supplier;

/**
 * Handler for the Task Reactor instance resuming process. A new instance of the handler must be
 * created using {@link ResumeInstanceHandlerBuilder the builder}.
 */
public class ResumeInstanceHandler {

  /**
   * Error type for the connection configuration failure, used by the {@link ConnectorErrorHelper}.
   */
  public static final String ERROR_TYPE = "RESUME_TASK_REACTOR_INSTANCE_FAILED";

  private final ConnectorErrorHelper errorHelper;
  private final ResumeTaskReactorService resumeTaskReactorService;

  ResumeInstanceHandler(
      ConnectorErrorHelper errorHelper, ResumeTaskReactorService resumeTaskReactorService) {
    this.resumeTaskReactorService = resumeTaskReactorService;
    this.errorHelper = errorHelper;
  }

  /**
   * Default handler method for the {@code TASK_REACTOR.RESUME_INSTANCE} procedure.
   *
   * <p>It resumes the instance dispatcher task and inserts the {@code RESUME_INSTANCE} command into
   * the Task Reactor command queue.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance name
   * @return a variant representing the {@link ConnectorResponse} returned by {@link
   *     #resumeInstance(Identifier) resumeInstance}
   */
  public static Variant resumeInstance(Session session, String instanceSchema) {
    Identifier identifier = Identifier.fromWithAutoQuoting(instanceSchema);
    return new ResumeInstanceHandlerBuilder(session).build().resumeInstance(identifier).toVariant();
  }

  /**
   * Executes the main logic of the handler, with logging using {@link
   * ConnectorErrorHelper#withExceptionLogging(Supplier) withExceptionLogging}.
   *
   * @param instanceSchema Task Reactor instance name
   * @return a response with the code {@code OK} if the execution was successful, otherwise a
   *     response with an error code and an error message
   */
  public ConnectorResponse resumeInstance(Identifier instanceSchema) {
    return errorHelper.withExceptionLoggingAndWrapping(
        () -> {
          resumeTaskReactorService.resumeInstance(instanceSchema);
          return ConnectorResponse.success();
        });
  }
}
