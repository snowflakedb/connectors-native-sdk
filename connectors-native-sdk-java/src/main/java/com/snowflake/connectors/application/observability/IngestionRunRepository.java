/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability;

import com.snowflake.connectors.application.observability.exception.IngestionRunsBadStatusException;
import com.snowflake.connectors.application.observability.exception.IngestionRunsReferenceException;
import com.snowflake.connectors.application.observability.exception.IngestionRunsUpdateException;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Optional;

/**
 * A repository for basic storage of the ingestion run information. This repository should only be
 * used for logging purposes.
 */
public interface IngestionRunRepository {

  /**
   * Starts a new ingestion run with the provided definition id, configuration id, process id, and
   * metadata.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration definition id
   * @param ingestionProcessId ingestion process id
   * @param metadata ingestion run metadata
   * @return id of the started ingestion run
   * @throws IngestionRunsReferenceException if insufficient ingestion information is provided
   */
  String startRun(
      String resourceIngestionDefinitionId,
      String ingestionConfigurationId,
      String ingestionProcessId,
      Variant metadata);

  /**
   * Starts a new ingestion run with the provided definition id, configuration id, and metadata.
   *
   * <p>No ingestion process id is used for the new ingestion run.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @param ingestionConfigurationId ingestion configuration definition id
   * @param metadata ingestion run metadata
   * @return id of the started ingestion run
   * @throws IngestionRunsReferenceException if insufficient ingestion information is provided
   */
  default String startRun(
      String resourceIngestionDefinitionId, String ingestionConfigurationId, Variant metadata) {
    return startRun(resourceIngestionDefinitionId, ingestionConfigurationId, null, metadata);
  }

  /**
   * Ends the specified ingestion run, setting the run status, number of ingested rows, and
   * metadata.
   *
   * @param id ingestion run id
   * @param status ingestion run status
   * @param ingestedRows number of ingested rows
   * @param mode specifies how the number of ingested rows will be persisted
   * @param metadata ingestion run metadata
   * @throws IngestionRunsBadStatusException if the provided status cannot be used when ending an
   *     ingestion run
   */
  void endRun(
      String id,
      IngestionRun.IngestionStatus status,
      Long ingestedRows,
      Mode mode,
      Variant metadata);

  /**
   * Ends the specified ingestion run, setting the run status, number of ingested rows, and
   * metadata.
   *
   * <p>The currently persisted number of ingested rows will be overwritten by the provided value.
   *
   * @param id ingestion run id
   * @param status ingestion run status
   * @param ingestedRows number of ingested rows
   * @param metadata ingestion run metadata
   * @throws IngestionRunsBadStatusException if the provided status cannot be used when ending an
   *     ingestion run
   */
  default void endRun(
      String id, IngestionRun.IngestionStatus status, Long ingestedRows, Variant metadata) {
    endRun(id, status, ingestedRows, Mode.OVERWRITE, metadata);
  }

  /**
   * Updates the number of ingested rows for the specified ingestion run.
   *
   * @param id ingestion run id
   * @param ingestedRows number of ingested rows
   * @param mode specifies how the number of ingested rows will be persisted
   * @throws IngestionRunsUpdateException if the specified ingestion run does not exist
   */
  void updateIngestedRows(String id, Long ingestedRows, Mode mode);

  /**
   * Updates the number of ingested rows for the specified ingestion run.
   *
   * <p>The currently persisted number of ingested rows will be overwritten by the provided value.
   *
   * @param id ingestion run id
   * @param ingestedRows number of ingested rows
   * @throws IngestionRunsUpdateException if the specified ingestion run does not exist
   */
  default void updateIngestedRows(String id, Long ingestedRows) {
    updateIngestedRows(id, ingestedRows, Mode.OVERWRITE);
  }

  /**
   * Finds an ingestion run with the specified id.
   *
   * @param id id of the ingestion run
   * @return ingestion run with the specified id
   */
  Optional<IngestionRun> findById(String id);

  /**
   * Fetches all ingestion runs created for the specified resource ingestion definition id.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @return all ingestion runs created for the specified resource ingestion definition id
   */
  List<IngestionRun> fetchAllByResourceId(String resourceIngestionDefinitionId);

  /**
   * Fetches all ingestion runs created for the specified ingestion process id.
   *
   * @param processId ingestion process id
   * @return all ingestion runs created for the specified ingestion process id
   */
  List<IngestionRun> fetchAllByProcessId(String processId);

  /**
   * Deletes all ingestion runs with the specified resource ingestion definition id.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   */
  void deleteAllByResourceId(String resourceIngestionDefinitionId);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses the {@code STATE.INGESTION_RUN} table.
   *
   * <p>Default implementation of the repository also implements additional methods from the {@link
   * CrudIngestionRunRepository} interface.
   *
   * @param session Snowpark session object
   * @return a new repository instance
   */
  static IngestionRunRepository getInstance(Session session) {
    return new DefaultIngestionRunRepository(session);
  }

  /** Update mode for the number of rows ingested during an ingestion run. */
  enum Mode {

    /** Overwrite the currently persisted number of rows with the new value. */
    OVERWRITE,

    /** Add the new value to the currently persisted number of rows. */
    ADD
  }
}
