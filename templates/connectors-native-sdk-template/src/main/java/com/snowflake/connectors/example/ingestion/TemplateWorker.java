/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import com.snowflake.connectors.application.integration.SchedulerTaskReactorOnIngestionFinishedCallback;
import com.snowflake.connectors.application.observability.DefaultIngestionRunRepository;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.example.configuration.connection.TemplateConnectionConfigurationCallback;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.ingestion.IngestionWorker;
import com.snowflake.snowpark_java.Session;

/**
 * Backend implementation for the custom {@code PUBLIC.TEMPLATE_WORKER} procedure, used by the task
 * reactor to generate random data.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * TemplateConnectionConfigurationCallback} first.
 */
public class TemplateWorker {

  public static String executeWork(Session session, int workerId, String taskReactorSchema) {
    var ingestionRunRepository = new DefaultIngestionRunRepository(session);
    var callback = SchedulerTaskReactorOnIngestionFinishedCallback.getInstance(session);

    var ingestion = new TemplateIngestion(session, ingestionRunRepository, callback);
    var workerIdentifier = new WorkerId(workerId);
    var schemaIdentifier = Identifier.from(taskReactorSchema);
    var worker = IngestionWorker.from(session, ingestion, workerIdentifier, schemaIdentifier);

    try {
      worker.run();
      return "Executed";
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage());
    }
  }
}
