/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.ingestion;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.ingestion.IngestionWorker;
import com.snowflake.snowpark_java.Session;

public class BenchmarkWorker {

  public static String executeWork(Session session, int workerId, String taskReactorSchema) {
    var ingestion = new BenchmarkIngestion();
    var workerIdentifier = new WorkerId(workerId);
    var schemaIdentifier = Identifier.from(taskReactorSchema);
    var worker = IngestionWorker.from(session, ingestion, workerIdentifier, schemaIdentifier);

    try {
      worker.run();
      return "Executed";
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage());
    }
  }
}
