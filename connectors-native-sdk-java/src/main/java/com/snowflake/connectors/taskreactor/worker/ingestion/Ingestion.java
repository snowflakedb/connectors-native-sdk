/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.ingestion;

import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;

/**
 * Interface that should be implemented by users of task reactor and passed over to {@link
 * IngestionWorker} instance.
 *
 * @param <STATE> Type defining the state used in the ingestion.
 */
public interface Ingestion<STATE> {

  /**
   * Used to initialize or load the initial state for the ingestion. State returned from this method
   * is used in the first call of {@link Ingestion#performIngestion(WorkItem, Object)} method.
   *
   * @param workItem work item provided by the dispatcher
   * @return initial ingestion state
   */
  STATE initialState(WorkItem workItem);

  /**
   * Logic that needs to be executed before ingestion begins.
   *
   * @param workItem work item provided by the dispatcher
   */
  void preIngestion(WorkItem workItem);

  /**
   * This is where the actual data ingestion should happen.
   *
   * @param state State that was returned from the previous {@link
   *     Ingestion#performIngestion(WorkItem, Object) performIngestion} call, or the initial state
   *     if it's the first call of this method.
   * @param workItem work item provided by the dispatcher
   * @return new state that will be used as a state argument in the next call of this method
   */
  STATE performIngestion(WorkItem workItem, STATE state);

  /**
   * The worker calls {@link Ingestion#performIngestion(WorkItem, Object)} as long as the value
   * returned from this method is {@code true}.
   *
   * <p>Note that {@link Ingestion#performIngestion(WorkItem, Object)} is called at least once.
   *
   * @param workItem work item provided by the dispatcher
   * @param state current ingestion state
   * @return whether the ingestion has been completed
   */
  boolean isIngestionCompleted(WorkItem workItem, STATE state);

  /**
   * Logic that needs to be executed after ingestion ends.
   *
   * <p>Note: this is NOT called when the ingestion gets cancelled.
   *
   * @param workItem work item provided by the dispatcher
   * @param state current ingestion state
   */
  void postIngestion(WorkItem workItem, STATE state);

  /**
   * This method is invoked in case when the ingestion gets cancelled in the middle of the
   * execution. If that happens, {@link Ingestion#postIngestion(WorkItem, Object)} is NOT called
   * anymore.
   *
   * @param workItem work item provided by the dispatcher
   * @param lastState The newest state that was returned from {@link
   *     Ingestion#performIngestion(WorkItem, Object)} or {@link Ingestion#initialState(WorkItem)}.
   */
  default void ingestionCancelled(WorkItem workItem, STATE lastState) {}
}
