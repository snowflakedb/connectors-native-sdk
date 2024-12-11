/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.integration;

import com.snowflake.connectors.application.ingestion.definition.VariantResource;
import com.snowflake.connectors.application.integration.TaskReactorOnIngestionScheduledCallback;
import com.snowflake.connectors.application.scheduler.RunSchedulerIterationHandler;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.example.ConnectorObjects;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.RUN_SCHEDULER_ITERATION} procedure, used by
 * the scheduler system.
 */
public class SchedulerIntegratedWithTaskReactorHandler {

  public static Variant runIteration(Session session) {
    var callback =
        TaskReactorOnIngestionScheduledCallback.getInstance(
            session,
            Identifier.from(ConnectorObjects.TASK_REACTOR_INSTANCE),
            VariantResource.class);
    return RunSchedulerIterationHandler.builder(session)
        .withCallback(callback)
        .build()
        .runIteration()
        .toVariant();
  }
}
