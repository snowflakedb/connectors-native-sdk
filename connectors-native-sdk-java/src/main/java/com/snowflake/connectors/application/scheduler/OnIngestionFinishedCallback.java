/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import com.snowflake.connectors.application.ingestion.process.CrudIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.DefaultIngestionProcessRepository;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Callback called when ingestion has been finished.
 *
 * <p>Default implementation of this callback updates metadata of the finished ingestion process,
 * and:
 *
 * <ul>
 *   <li>if current status of the ingestion process is {@code IN_PROGRESS}, then next iteration is
 *       scheduled by changing the status to {@code SCHEDULED}
 *   <li>if current status of the ingestion process is {@code COMPLETED}, then next iteration is not
 *       scheduled and the status is preserved
 * </ul>
 */
public interface OnIngestionFinishedCallback {

  /**
   * Action executed when the next ingestion is finished.
   *
   * @param processId ingestion process id
   * @param metadata new ingestion process metadata
   */
  void onIngestionFinished(String processId, Variant metadata);

  /**
   * Returns a new instance of the default callback implementation.
   *
   * <p>Default implementation of the callback uses a default implementation of {@link
   * CrudIngestionProcessRepository}.
   *
   * @param session Snowpark session object
   * @return a new callback instance
   */
  static OnIngestionFinishedCallback getInstance(Session session) {
    CrudIngestionProcessRepository ingestionProcessRepository =
        new DefaultIngestionProcessRepository(session);
    return new DefaultOnIngestionFinishedCallback(ingestionProcessRepository);
  }
}
