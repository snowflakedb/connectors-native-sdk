/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.observability;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Session;
import java.util.List;
import java.util.Optional;

/**
 * A repository for basic storage of the ingestion run information. Unlike {@link
 * IngestionRunRepository} this repository should only provide simple CRUD operations, treating the
 * provided/fetched data as is, without any additional logic.
 */
public interface CrudIngestionRunRepository {

  /**
   * Saves the provided ingestion run.
   *
   * <p>Since the {@code startedAt} and {@code updatedAt} properties of the run are required, they
   * will be set to default values if not provided, despite the simple nature of this repository.
   *
   * @param ingestionRun ingestion run
   */
  void save(IngestionRun ingestionRun);

  /**
   * Finds all ingestion runs satisfying the specified condition.
   *
   * @param condition ingestion run condition
   * @return all ingestion runs satisfying the specified condition
   */
  List<IngestionRun> findWhere(Column condition);

  /**
   * Finds first ingestion run matching specified condition from sorted result.
   *
   * @param condition condition matching ingestion runs
   * @param sorted sorting order
   * @return first ingestion run matching specified condition from sorted result
   */
  Optional<IngestionRun> findBy(Column condition, Column sorted);

  /**
   * Finds all ingestion runs with the status of {@link
   * com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus#IN_PROGRESS
   * IN_PROGRESS}.
   *
   * @return all ongoing ingestion runs
   */
  List<IngestionRun> findOngoingIngestionRuns();

  /**
   * Finds all ingestion runs created for the specified resource ingestion definition id and with
   * the status of {@link
   * com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus#IN_PROGRESS
   * IN_PROGRESS}.
   *
   * @param resourceIngestionDefinitionId resource ingestion definition id
   * @return all ongoing ingestion runs created for the specified resource ingestion definition id
   */
  List<IngestionRun> findOngoingIngestionRuns(String resourceIngestionDefinitionId);

  /**
   * Finds all ingestion runs satisfying the specified condition and with the status of {@link
   * com.snowflake.connectors.application.observability.IngestionRun.IngestionStatus#IN_PROGRESS
   * IN_PROGRESS}.
   *
   * @param condition condition for the ingestion run
   * @return all ongoing ingestion runs satisfying the specified condition
   */
  List<IngestionRun> findOngoingIngestionRunsWhere(Column condition);

  /**
   * Returns a new instance of the default repository implementation.
   *
   * <p>Default implementation of the repository uses the {@code STATE.INGESTION_RUN} table.
   *
   * <p>Default implementation of the repository also implements additional methods from the {@link
   * IngestionRunRepository} interface.
   *
   * @param session Snowpark session object
   * @return a new repository instance
   */
  static CrudIngestionRunRepository getInstance(Session session) {
    return new DefaultIngestionRunRepository(session);
  }
}
