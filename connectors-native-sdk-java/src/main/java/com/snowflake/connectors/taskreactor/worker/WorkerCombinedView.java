/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Session;
import java.util.List;
import java.util.stream.Stream;

/** Utility for creating a combined view of worker queues. */
public interface WorkerCombinedView {

  /**
   * Returns the ids of workers executing ingestion of a specified resource.
   *
   * @param ids Identifiers of work queue items
   * @return list of ids of workers executing ingestion of a specified resource
   */
  List<WorkerId> getWorkersExecuting(List<String> ids);

  /**
   * Returns the ids of workers executing ingestion of a specified resource.
   *
   * @param resourceId resource id
   * @return list of ids of workers executing ingestion of a specified resource
   */
  Stream<WorkerId> getWorkersExecuting(String resourceId);

  /**
   * Replaces current view, with actual state of queues.
   *
   * @param workerIds identifiers of currently active workers
   */
  void recreate(List<WorkerId> workerIds);

  /**
   * Returns a new instance of the default view implementation.
   *
   * <p>Default implementation of the view uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkerCombinedView DefaultWorkerCombinedView},
   *       created for the view {@code <schema>.WORKER_QUEUES} table.
   * </ul>
   *
   * @param session Snowpark session object
   * @param schema Schema of the Task Reactor
   * @return a new view instance
   */
  static WorkerCombinedView getInstance(Session session, Identifier schema) {
    return new DefaultWorkerCombinedView(session, schema);
  }
}
