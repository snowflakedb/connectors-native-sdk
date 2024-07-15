/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.IN_PROGRESS;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.connectors.taskreactor.telemetry.TaskReactorTelemetry;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import java.time.Instant;
import org.slf4j.Logger;

/**
 * A set of methods performed by the Worker implementation.
 *
 * @param <T> param defining type of response during running worker jobs.
 */
public abstract class Worker<T> {

  private static final Logger LOG = TaskReactorLogger.getLogger(Worker.class);

  private final WorkerId workerId;
  private final Identifier instanceName;
  private final WorkerStatusRepository workerStatusRepository;
  private final ObjectName workerTask;
  private final WorkerQueue workerQueue;
  private final TaskRepository taskRepository;

  protected Worker(
      WorkerId workerId,
      Identifier instanceName,
      WorkerStatusRepository workerStatusRepository,
      ObjectName workerTask,
      WorkerQueue workerQueue,
      TaskRepository taskRepository) {
    this.workerId = workerId;
    this.instanceName = instanceName;
    this.workerStatusRepository = workerStatusRepository;
    this.workerTask = workerTask;
    this.workerQueue = workerQueue;
    this.taskRepository = taskRepository;
  }

  /**
   * Generic body of the worker execution job.
   *
   * @return result of the {@link #performWork} method implementation.
   * @throws WorkerException when generic worker fail occurs.
   * @throws WorkerJobCancelledException if running task is performed by worker scheduled for
   *     cancellation.
   */
  public T run() throws WorkerException, WorkerJobCancelledException {
    LOG.debug("Worker started working (workerId: {})", workerId.value());

    Instant workStartTime = Instant.now();
    TaskReactorTelemetry.setTaskReactorInstanceNameSpanAttribute(instanceName);
    TaskReactorTelemetry.setWorkerIdSpanAttribute(workerId);

    workerStatusRepository
        .getLastAvailable(workerId)
        .ifPresent(
            timestamp -> TaskReactorTelemetry.addWorkerIdleTimeEvent(timestamp, workStartTime));

    try {
      if (shouldCancel()) {
        throw new WorkerJobCancelledException();
      }

      workerStatusRepository.updateStatusFor(workerId, IN_PROGRESS);
      WorkItem workItem = workerQueue.fetch(workerId);
      LOG.info("Worker (workerId: {}) started work on workItem ({})", workerId.value(), workItem);
      T result = performWork(workItem);
      LOG.info("Worker (workerId: {}) completed work on workItem ({})", workerId.value(), workItem);
      return result;
    } catch (WorkerJobCancelledException e) {
      LOG.info("Worker's job cancelled (workerId: {})", workerId.value());
      throw e;
    } catch (Exception e) {
      LOG.error("Worker (workerId: {}) failed", workerId.value());
      throw new WorkerException(String.format("Worker %s failed", workerId), e);
    } finally {
      taskRepository.fetch(workerTask).suspend();
      workerQueue.delete(workerId);
      workerStatusRepository.updateStatusFor(workerId, AVAILABLE);
      TaskReactorTelemetry.addWorkerWorkingTimeEvent(workStartTime, Instant.now());
      LOG.debug("Worker finished working (workerId: {})", workerId.value());
    }
  }

  /**
   * @param workItem work item provided by the dispatcher
   * @return custom object defined by the user.
   */
  protected abstract T performWork(WorkItem workItem);

  protected boolean shouldCancel() {
    return workerStatusRepository.getStatusFor(workerId) == SCHEDULED_FOR_CANCELLATION;
  }
}
