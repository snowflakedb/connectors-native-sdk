/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.CANCELED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.COMPLETED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.FAILED;
import static com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus.IN_PROGRESS;
import static com.snowflake.connectors.example.ConnectorObjects.DATA_TABLE;

import com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus;
import com.snowflake.connectors.application.observability.IngestionRunRepository;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.connectors.example.configuration.utils.ConnectorConfigurationPropertyNotFoundException;
import com.snowflake.connectors.taskreactor.OnIngestionFinishedCallback;
import com.snowflake.connectors.taskreactor.worker.ingestion.Ingestion;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.snowflake.client.jdbc.internal.google.cloud.Timestamp;

/** Custom implementation of {@link Ingestion}, used for ingestion of random data */
public class TemplateIngestion implements Ingestion<IngestionStatus> {

  private final Session session;
  private final IngestionRunRepository ingestionRunRepository;
  private final OnIngestionFinishedCallback onIngestionFinishedCallback;

  private String ingestionRunId;
  private TemplateWorkItem templateWorkItem;
  private long ingestedRows = 0L;

  public TemplateIngestion(
      Session session,
      IngestionRunRepository ingestionRunRepository,
      OnIngestionFinishedCallback onIngestionFinishedCallback) {
    this.session = session;
    this.ingestionRunRepository = ingestionRunRepository;
    this.onIngestionFinishedCallback = onIngestionFinishedCallback;
  }

  @Override
  public IngestionStatus initialState(WorkItem workItem) {
    this.templateWorkItem = TemplateWorkItem.from(workItem);
    return IN_PROGRESS;
  }

  @Override
  public void preIngestion(WorkItem workItem) {
    this.ingestionRunId =
        ingestionRunRepository.startRun(
            templateWorkItem.getResourceIngestionDefinitionId(),
            templateWorkItem.getIngestionConfigurationId(),
            templateWorkItem.getProcessId(),
            null);
  }

  @Override
  public IngestionStatus performIngestion(WorkItem workItem, IngestionStatus ingestionStatus) {
    var destinationTable = prepareDestinationTableName().getValue();

    try {
      // TODO: IMPLEMENT ME ingestion: The following data generates some random data to be stored.
      // In a real use case
      // scenario a service handling communication with the external source system should be called
      // here.
      List<Variant> rawResults = randomData();

      this.ingestedRows = IngestionHelper.saveRawData(this.session, destinationTable, rawResults);
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
    onIngestionFinishedCallback.onIngestionFinished(templateWorkItem.getProcessId(), null);
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
    return ObjectName.from(destinationDatabase, destinationSchema, DATA_TABLE);
  }

  public List<Variant> randomData() {
    return IntStream.range(0, new Random().nextInt(128))
        .boxed()
        .map(
            it ->
                new Variant(
                    Map.of(
                        "resource_id",
                        templateWorkItem.getResourceIngestionDefinitionId(),
                        "id",
                        UUID.randomUUID(),
                        "timestamp",
                        Timestamp.now().toString())))
        .collect(Collectors.toList());
  }
}
