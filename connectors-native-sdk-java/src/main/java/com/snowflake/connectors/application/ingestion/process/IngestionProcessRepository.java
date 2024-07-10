/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Optional;

/** A repository for basic storage of the ingestion processes. */
public interface IngestionProcessRepository {

  /**
   * Creates a new ingestion process.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   * @param status {@link IngestionProcessStatuses initial process status}
   * @param metadata process metadata
   * @return id of the created process
   */
  String createProcess(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status,
      Variant metadata);

  /**
   * Updates the status of an ingestion process with the specified id.
   *
   * @param processId process id
   * @param status {@link IngestionProcessStatuses new process status}
   */
  void updateStatus(String processId, String status);

  /**
   * Updates the status of an ingestion process with the specified resource ingestion definition id,
   * ingestion configuration, and process type.
   *
   * <p>This update method is not recommended, as technically multiple processes can fit the
   * specified criteria. It should only be used when the status update is a part of a resource
   * update.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   * @param status new process status
   */
  void updateStatus(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String type,
      String status);

  /**
   * Ends an ingestion process with the specified id.
   *
   * <p>The ending of the process will set the status to a terminal value and update the process'
   * finishedAt timestamp.
   *
   * @param processId process id
   */
  void endProcess(String processId);

  /**
   * Ends an ingestion process with the specified resource ingestion definition id, ingestion
   * configuration, and process type.
   *
   * <p>This update method is not recommended, as technically multiple processes can fit the
   * specified criteria. It should only be used when the status update is a part of a resource
   * update.
   *
   * <p>The ending of the process will set the status to a terminal value and update the process'
   * finishedAt timestamp.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   */
  void endProcess(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type);

  /**
   * Fetches an ingestion process by the specified id.
   *
   * @param processId process id
   * @return ingestion process with the specified id
   */
  Optional<IngestionProcess> fetch(String processId);

  /**
   * Fetches an ingestion process with the latest finishedAt date and status = FINISHED
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   * @return ingestion process with the specified id
   */
  Optional<IngestionProcess> fetchLastFinished(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type);

  /**
   * Fetches all ingestion processes with the specified resource ingestion definition id, ingestion
   * configuration, and process type.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration id
   * @param type process type
   * @return a list containing processes matching the specified criteria
   */
  List<IngestionProcess> fetchAll(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, String type);

  /**
   * Fetches all ingestion processes with the specified resource ingestion definition id and
   * statuses: {@link IngestionProcessStatuses#SCHEDULED}, {@link
   * IngestionProcessStatuses#IN_PROGRESS}.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @return a list containing processes matching the specified criteria
   */
  List<IngestionProcess> fetchAllActive(String resourceIngestionDefinitionId);
}
