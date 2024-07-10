/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.CANCELED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.COMPLETED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.FAILED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.IN_PROGRESS;
import static com.snowflake.connectors.example.ConnectorObjects.ISSUES_TABLE;

import com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus;
import com.snowflake.connectors.application.observability.IngestionRunRepository;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.connectors.example.configuration.utils.ConnectorConfigurationPropertyNotFoundException;
import com.snowflake.connectors.example.http.GithubHttpHelper;
import com.snowflake.connectors.taskreactor.OnIngestionFinishedCallback;
import com.snowflake.connectors.taskreactor.worker.ingestion.Ingestion;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.Session;

/** Custom implementation of {@link Ingestion}, used for ingestion of GitHub repository issues. */
public class GitHubIngestion implements Ingestion<IngestionStatus> {

  private final Session session;
  private final IngestionRunRepository ingestionRunRepository;
  private final OnIngestionFinishedCallback onIngestionFinishedCallback;

  private String ingestionRunId;
  private GitHubWorkItem gitHubWorkItem;
  private long ingestedRows = 0L;

  public GitHubIngestion(
      Session session,
      IngestionRunRepository ingestionRunRepository,
      OnIngestionFinishedCallback onIngestionFinishedCallback) {
    this.session = session;
    this.ingestionRunRepository = ingestionRunRepository;
    this.onIngestionFinishedCallback = onIngestionFinishedCallback;
  }

  @Override
  public IngestionStatus initialState(WorkItem workItem) {
    this.gitHubWorkItem = GitHubWorkItem.from(workItem);
    return IN_PROGRESS;
  }

  @Override
  public void preIngestion(WorkItem workItem) {
    this.ingestionRunId =
        ingestionRunRepository.startRun(
            gitHubWorkItem.getResourceIngestionDefinitionId(),
            gitHubWorkItem.getIngestionConfigurationId(),
            gitHubWorkItem.getProcessId(),
            null);
  }

  @Override
  public IngestionStatus performIngestion(WorkItem workItem, IngestionStatus ingestionStatus) {
    var destinationTable = prepareDestinationTableName().getValue();

    try {
      var rawResults =
          GithubHttpHelper.fetchRepoIssues(
              gitHubWorkItem.getOrganization(), gitHubWorkItem.getRepository());
      this.ingestedRows =
          IngestionHelper.saveRawData(this.session, destinationTable, rawResults, gitHubWorkItem);
      return COMPLETED;
    } catch (Exception exception) {
      return FAILED;
    }
  }

  @Override
  public boolean isIngestionCompleted(WorkItem workItem, IngestionStatus status) {
    return status != IN_PROGRESS;
  }

  @Override
  public void postIngestion(WorkItem workItem, IngestionStatus ingestionStatus) {
    ingestionRunRepository.endRun(ingestionRunId, ingestionStatus, ingestedRows, null);
    onIngestionFinishedCallback.onIngestionFinished(gitHubWorkItem.getProcessId(), null);
  }

  @Override
  public void ingestionCancelled(WorkItem workItem, IngestionStatus lastState) {
    ingestionRunRepository.endRun(ingestionRunId, CANCELED, ingestedRows, null);
  }

  private ObjectName prepareDestinationTableName() {
    var connectorConfig = Configuration.fromConnectorConfig(session);
    var destinationDatabase =
        connectorConfig
            .getValue(DESTINATION_DATABASE.getPropertyName())
            .orElseThrow(
                () ->
                    new ConnectorConfigurationPropertyNotFoundException(
                        DESTINATION_DATABASE.getPropertyName()));
    var destinationSchema =
        connectorConfig
            .getValue(DESTINATION_SCHEMA.getPropertyName())
            .orElseThrow(
                () ->
                    new ConnectorConfigurationPropertyNotFoundException(
                        DESTINATION_SCHEMA.getPropertyName()));
    return ObjectName.from(destinationDatabase, destinationSchema, ISSUES_TABLE);
  }
}
