/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.ComponentNames.workerTask;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.WORK_ASSIGNED;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.commands.processor.CommandsProcessor;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.connectors.taskreactor.queue.cleaner.QueueItemCleaner;
import com.snowflake.connectors.taskreactor.queue.selector.QueueItemSelector;
import com.snowflake.connectors.taskreactor.telemetry.TaskReactorTelemetry;
import com.snowflake.connectors.taskreactor.worker.WorkerCombinedView;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.WorkerManager;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

/** Main task reactor task dispatcher. */
public class Dispatcher {

  private static final Logger LOG = TaskReactorLogger.getLogger(Dispatcher.class);

  private final Identifier instanceSchema;
  private final WorkerStatusRepository workerStatusRepository;
  private final WorkerManager workerManager;
  private final WorkerCombinedView workerCombinedView;
  private final WorkItemQueue workItemQueue;
  private final WorkerQueue workerQueue;
  private final TaskRepository taskRepository;
  private final CommandsProcessor commandsProcessor;
  private final QueueItemSelector queueItemSelector;
  private final QueueItemCleaner queueItemCleaner;
  private final InstanceStreamService instanceStreamService;

  /**
   * Create a new {@link Dispatcher}.
   *
   * @param session Snowpark session object
   * @param instanceSchema Task Reactor instance name
   * @return a new dispatcher
   */
  public static Dispatcher from(Session session, Identifier instanceSchema) {
    return new Dispatcher(
        instanceSchema,
        WorkerStatusRepository.getInstance(session, instanceSchema),
        WorkerManager.from(session, instanceSchema),
        WorkerCombinedView.getInstance(session, instanceSchema),
        WorkItemQueue.getInstance(session, instanceSchema),
        WorkerQueue.getInstance(session, instanceSchema),
        TaskRepository.getInstance(session),
        CommandsProcessor.getInstance(session, instanceSchema),
        QueueItemSelector.from(session, instanceSchema),
        QueueItemCleaner.from(session, instanceSchema),
        InstanceStreamService.getInstance(session));
  }

  Dispatcher(
      Identifier instanceSchema,
      WorkerStatusRepository workerStatusRepository,
      WorkerManager workerManager,
      WorkerCombinedView workerCombinedView,
      WorkItemQueue workItemQueue,
      WorkerQueue workerQueue,
      TaskRepository taskRepository,
      CommandsProcessor commandsProcessor,
      QueueItemSelector queueItemSelector,
      QueueItemCleaner queueItemCleaner,
      InstanceStreamService instanceStreamService) {
    this.instanceSchema = instanceSchema;
    this.workerStatusRepository = workerStatusRepository;
    this.workerManager = workerManager;
    this.workerCombinedView = workerCombinedView;
    this.workItemQueue = workItemQueue;
    this.workerQueue = workerQueue;
    this.taskRepository = taskRepository;
    this.commandsProcessor = commandsProcessor;
    this.queueItemSelector = queueItemSelector;
    this.queueItemCleaner = queueItemCleaner;
    this.instanceStreamService = instanceStreamService;
  }

  /**
   * Executes the next dispatcher run.
   *
   * @return work items dispatching result
   */
  public String execute() {
    LOG.trace("Dispatcher started working (instance: {})", instanceSchema.getValue());
    TaskReactorTelemetry.setTaskReactorInstanceNameSpanAttribute(instanceSchema);

    commandsProcessor.processCommands();
    instanceStreamService.recreateStreamsIfRequired(instanceSchema);
    workerManager.reconcileWorkersNumber();
    cancelOngoingExecutions();
    queueItemCleaner.clean();

    String dispatchingResult = dispatchWorkItems();
    TaskReactorTelemetry.addWorkerStatusEvent(
        workerStatusRepository.getWorkersNumberForEachStatus());
    return dispatchingResult;
  }

  private void cancelOngoingExecutions() {
    workItemQueue.fetchNotProcessedAndCancelingItems().stream()
        .filter(item -> item.cancelOngoingExecution)
        .forEach(this::cancelOngoingExecution);
  }

  /**
   * Cancels ongoing executions for items with the same resource id as a provided queue item.
   *
   * @param queueItem queue item
   */
  public void cancelOngoingExecution(QueueItem queueItem) {
    workItemQueue.deleteBefore(queueItem.resourceId, queueItem.timestamp);
    workerCombinedView
        .getWorkersExecuting(queueItem.resourceId)
        .forEach(
            workerId ->
                workerStatusRepository.updateStatusFor(workerId, SCHEDULED_FOR_CANCELLATION));
  }

  private String dispatchWorkItems() {
    List<QueueItem> selectedQueueItems = queueItemSelector.getSelectedItemsFromQueue();

    if (selectedQueueItems.isEmpty()) {
      LOG.trace(
          "Work Selector selected no items for processing (instance: {})",
          instanceSchema.getValue());
      return "Work Selector selected no items for processing. Exiting.";
    }

    List<WorkerId> availableWorkers = new ArrayList<>(workerStatusRepository.getAvailableWorkers());
    List<QueueItem> itemsToDispatch =
        selectedQueueItems.stream().limit(availableWorkers.size()).collect(toList());

    for (int i = 0; i < itemsToDispatch.size(); i++) {
      dispatchItem(itemsToDispatch.get(i), availableWorkers.get(i));
    }

    return String.format("Consumed %d items from the queue.", itemsToDispatch.size());
  }

  private void dispatchItem(QueueItem item, WorkerId workerId) {
    LOG.info(
        "Dispatching workItem (id: {}, resourceId: {}) to worker (workerId: {}) (instance: {})",
        item.id,
        item.resourceId,
        workerId,
        instanceSchema.getValue());
    ObjectName workerTask = ObjectName.from(instanceSchema, workerTask(workerId));

    workerQueue.push(item, workerId);
    workerStatusRepository.updateStatusFor(workerId, WORK_ASSIGNED);
    taskRepository.fetch(workerTask).resume();
    workItemQueue.delete(item.id);

    TaskReactorTelemetry.addWorkItemWaitingInQueueTimeEvent(item.timestamp, Instant.now());
  }
}
