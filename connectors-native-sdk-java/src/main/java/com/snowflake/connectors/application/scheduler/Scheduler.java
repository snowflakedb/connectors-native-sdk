/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.scheduler;

import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.IN_PROGRESS;
import static com.snowflake.connectors.application.ingestion.process.IngestionProcessStatuses.SCHEDULED;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.application.ingestion.process.CrudIngestionProcessRepository;
import com.snowflake.connectors.application.ingestion.process.IngestionProcess;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.util.snowflake.TransactionManager;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ingestion process scheduler. */
public class Scheduler {

  /** Name of the scheduler task object. */
  public static final ObjectName SCHEDULER_TASK = ObjectName.from("STATE", "SCHEDULER_TASK");

  private static final Logger logger = LoggerFactory.getLogger("SCHEDULER");

  private final CrudIngestionProcessRepository ingestionProcessRepository;
  private final OnIngestionScheduledCallback onIngestionScheduledCallback;
  private final TransactionManager transactionManager;

  /**
   * Creates a new {@link Scheduler}.
   *
   * @param ingestionProcessRepository ingestion process repository
   * @param onIngestionScheduledCallback callback called when the next scheduler iteration is run
   * @param transactionManager transaction manager
   */
  public Scheduler(
      CrudIngestionProcessRepository ingestionProcessRepository,
      OnIngestionScheduledCallback onIngestionScheduledCallback,
      TransactionManager transactionManager) {
    this.ingestionProcessRepository = ingestionProcessRepository;
    this.onIngestionScheduledCallback = onIngestionScheduledCallback;
    this.transactionManager = transactionManager;
  }

  /** Runs the next scheduler iteration. */
  public void runIteration() {
    List<IngestionProcess> processes = ingestionProcessRepository.fetchAll(SCHEDULED);
    if (!processes.isEmpty()) {
      transactionManager.withTransaction(
          () -> {
            changeStatusToInProgress(processes);
            callOnIngestionScheduled(processes);
          });
    }
  }

  private void changeStatusToInProgress(List<IngestionProcess> processes) {
    List<IngestionProcess> updatedProcesses =
        processes.stream().map(process -> process.withStatus(IN_PROGRESS)).collect(toList());
    ingestionProcessRepository.save(updatedProcesses);
  }

  private void callOnIngestionScheduled(List<IngestionProcess> processes) {
    List<String> processIds = processes.stream().map(IngestionProcess::getId).collect(toList());
    onIngestionScheduledCallback.onIngestionScheduled(processIds);
  }
}
