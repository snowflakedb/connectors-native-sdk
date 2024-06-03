/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.process;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A repository for basic storage of the ingestion processes. Unlike {@link
 * IngestionProcessRepository} this repository should only provide simple CRUD operations, treating
 * the provided/fetched data as is, without any additional logic.
 */
public interface CrudIngestionProcessRepository {

  /**
   * Saves the specified ingestion process.
   *
   * @param ingestionProcess ingestion process
   */
  void save(IngestionProcess ingestionProcess);

  /**
   * Saves the specified ingestion processes.
   *
   * @param ingestionProcesses ingestion processes
   */
  void save(Collection<IngestionProcess> ingestionProcesses);

  /**
   * Fetches an ingestion process by the specified id.
   *
   * @param processId process id
   * @return ingestion process with the specified id
   */
  Optional<IngestionProcess> fetch(String processId);

  /**
   * Fetches all ingestion processes with resource ingestion definition id contained within the
   * specified list of ids.
   *
   * @param resourceIngestionDefinitionIds resource ingestion definition ids
   * @return a list containing processes matching the specified criteria
   */
  List<IngestionProcess> fetchAll(List<String> resourceIngestionDefinitionIds);

  /**
   * Fetches all ingestion processes with the specified status.
   *
   * @param status process status
   * @return a list containing all the processes matching the specified criteria
   */
  List<IngestionProcess> fetchAll(String status);
}
