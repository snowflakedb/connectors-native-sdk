/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that should be implemented by users of task reactor and passed over to {@link
 * IngestionWorker} instance.
 *
 * @param <I> resource id class, containing properties which identify the resource in the source
 *     system
 * @param <M> resource metadata class, containing additional properties which identify the resource
 *     in the source system. The properties can be fetched automatically or calculated by the
 *     connector
 * @param <C> custom ingestion configuration class, containing custom ingestion properties
 * @param <D> destination configuration class, containing properties describing where the ingested
 *     data should be stored
 */
public interface SimpleIngestion<I, M, C, D> {

  /** Logger instance for use in the default methods of this interface. */
  Logger log = LoggerFactory.getLogger(SimpleIngestion.class);

  /**
   * Fetches data from source, can be also responsible for creating necessary objects like tables.
   * It is optional step since data can be fetched in a persistData method.
   *
   * @param workItem work item provided by the dispatcher.
   * @param ingestionRunId ingestion run id created by the worker.
   */
  default void fetchData(SimpleIngestionWorkItem<I, M, C, D> workItem, String ingestionRunId) {
    log.debug(
        "Calling empty fetchData for work item: {}, ingestionRunId: {}", workItem, ingestionRunId);
  }

  /**
   * Persists data in the destination. Data can be fetched here if it must be done in one
   * transactional step.
   *
   * @param workItem work item provided by the dispatcher.
   * @param ingestionRunId ingestion run id created by the worker.
   * @return number of rows persisted to the destination.
   */
  long persistData(SimpleIngestionWorkItem<I, M, C, D> workItem, String ingestionRunId);

  /**
   * Executes callback when successful ingestion is registered.
   *
   * @param workItem work item provided by the dispatcher.
   * @param ingestionRunId ingestion run id created by the worker.
   */
  default void onSuccessfulIngestionCallback(
      SimpleIngestionWorkItem<I, M, C, D> workItem, String ingestionRunId) {}

  /**
   * Executes callback when failed ingestion is registered.
   *
   * @param workItem work item provided by the dispatcher.
   * @param ingestionRunId ingestion run id created by the worker.
   */
  default void onFailedIngestionCallback(
      SimpleIngestionWorkItem<I, M, C, D> workItem, String ingestionRunId) {}

  /**
   * Does steps after successful ingestion.
   *
   * @param workItem work item provided by the dispatcher.
   * @param ingestionRunId ingestion run id created by the worker.
   */
  default void postIngestion(SimpleIngestionWorkItem<I, M, C, D> workItem, String ingestionRunId) {
    log.debug(
        "Calling empty postTransaction for work item: {}, ingestionRunId: {}",
        workItem,
        ingestionRunId);
  }
}
