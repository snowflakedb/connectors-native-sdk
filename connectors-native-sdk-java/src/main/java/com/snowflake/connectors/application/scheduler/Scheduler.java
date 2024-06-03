/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.application.ingestion.process.CrudIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.common.object.ObjectName;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ingestion process scheduler. */
public class Scheduler {

  public static final ObjectName SCHEDULER_TASK = ObjectName.from("STATE", "SCHEDULER_TASK");
  private static final Logger logger = LoggerFactory.getLogger("SCHEDULER");

  private final CrudIngestionProcessRepository ingestionProcessRepository;
  private final OnIngestionScheduledCallback onIngestionScheduledCallback;

  /**
   * Creates a new {@link Scheduler}.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @param onIngestionScheduledCallback callback called when the next scheduler iteration is run
   */
  public Scheduler(
      CrudIngestionProcessRepository ingestionProcessRepository,
      OnIngestionScheduledCallback onIngestionScheduledCallback) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.onIngestionScheduledCallback = onIngestionScheduledCallback;
  }

  /** Runs the next scheduler iteration. */
  public void runIteration() {
    List<IngestionProcess> processes = ingestionProcessRepository.fetchAll("SCHEDULED");
    if (!processes.isEmpty()) {
      changeStatusToInProgress(processes);
      processes.forEach(this::callOnIngestionScheduled);
    }
  }

  private void changeStatusToInProgress(List<IngestionProcess> processes) {
    List<IngestionProcess> updatedProcesses =
        processes.stream().map(process -> process.withStatus("IN_PROGRESS")).collect(toList());
    ingestionProcessRepository.save(updatedProcesses);
  }

  private void callOnIngestionScheduled(IngestionProcess process) {
    try {
      onIngestionScheduledCallback.onIngestionScheduled(process.getId());
    } catch (Exception e) {
      logger.error(
          "Unexpected error occurred when calling onIngestionScheduled callback for process with id"
              + " {}: {}",
          process.getId(),
          e.getMessage());
    }
  }
}
