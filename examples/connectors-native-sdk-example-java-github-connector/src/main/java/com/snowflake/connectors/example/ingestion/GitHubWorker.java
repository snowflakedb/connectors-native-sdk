/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import com.snowflake.connectors.application.integration.SchedulerTaskReactorOnIngestionFinishedCallback;
import com.snowflake.connectors.application.observability.DefaultIngestionRunRepository;
import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.example.configuration.connection.GithubConnectionConfigurationCallback;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.ingestion.IngestionWorker;
import com.snowflake.snowpark_java.Session;

/**
 * Backend implementation for the custom {@code PUBLIC.GITHUB_WORKER} procedure, used by the task
 * reactor instance for GitHub issues ingestion.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * GithubConnectionConfigurationCallback} first.
 */
public class GitHubWorker {

  public static String executeWork(Session session, int workerId, String taskReactorSchema) {
    var ingestionRunRepository = new DefaultIngestionRunRepository(session);
    var callback = SchedulerTaskReactorOnIngestionFinishedCallback.getInstance(session);

    var gitHubIngestion = new GitHubIngestion(session, ingestionRunRepository, callback);
    var workerIdentifier = new WorkerId(workerId);
    var schemaIdentifier = Identifier.fromWithAutoQuoting(taskReactorSchema);
    var worker = IngestionWorker.from(session, gitHubIngestion, workerIdentifier, schemaIdentifier);

    try {
      worker.run();
      return "Executed";
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage());
    }
  }
}
