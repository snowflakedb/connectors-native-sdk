/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_push_based_java_connector;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

class ResourceScheduler {

  private final ScheduledExecutorService executorService;
  private final Map<String, ScheduledFuture<?>> tasks;
  private final ObjectsInitializer objectsInitializer;
  private final IngestionService ingestionService;

  private final int initialUploadRecordCount;
  private final int periodicalUploadRecordCount;
  private final int periodicalUploadIntervalSeconds;

  ResourceScheduler(
      ObjectsInitializer objectsInitializer,
      IngestionService ingestionService,
      int initialUploadRecordCount,
      int periodicalUploadRecordCount,
      int periodicalUploadIntervalSeconds,
      int schedulerPoolSize) {
    this.ingestionService = ingestionService;
    this.objectsInitializer = objectsInitializer;
    this.initialUploadRecordCount = initialUploadRecordCount;
    this.periodicalUploadIntervalSeconds = periodicalUploadIntervalSeconds;
    this.periodicalUploadRecordCount = periodicalUploadRecordCount;

    this.executorService = Executors.newScheduledThreadPool(schedulerPoolSize);
    this.tasks = new HashMap<>();
  }

  void enableResource(String resourceName) {
    DataSource dataSource = new RandomDataSource();

    objectsInitializer.initResource(resourceName);
    initialUpload(resourceName, dataSource);
    schedulePeriodicalUpload(resourceName, dataSource);
  }

  private void initialUpload(String resourceName, DataSource dataSource) {
    String tableName = resourceName + "_BASE";
    var records = dataSource.fetchInitialRecords(initialUploadRecordCount);
    ingestionService.uploadRecords(tableName, records);
  }

  private void schedulePeriodicalUpload(String resourceName, DataSource dataSource) {
    String tableName = resourceName + "_DELTA";

    ScheduledFuture<?> task =
        executorService.scheduleAtFixedRate(
            () -> periodicalUpload(tableName, dataSource),
            0,
            periodicalUploadIntervalSeconds,
            SECONDS);
    tasks.put(resourceName, task);
  }

  private void periodicalUpload(String tableName, DataSource dataSource) {
    var records = dataSource.fetchRecords(periodicalUploadRecordCount);
    ingestionService.uploadRecords(tableName, records);
  }

  void disableResource(String resourceName) {
    ScheduledFuture<?> task = tasks.remove(resourceName);
    task.cancel(false);
  }

  void shutdown() throws Exception {
    executorService.shutdown();
    ingestionService.close();
  }
}
