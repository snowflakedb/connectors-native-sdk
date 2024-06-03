/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker;

import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.AVAILABLE;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.IN_PROGRESS;
import static com.snowflake.connectors.taskreactor.worker.status.WorkerStatus.SCHEDULED_FOR_CANCELLATION;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.telemetry.TaskReactorTelemetry;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import java.time.Instant;

/**
 * A set of methods performed by the Worker implementation.
 *
 * @param <T> param defining type of response during running worker jobs.
 */
public abstract class Worker<T> {

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
      return performWork(workerQueue.fetch(workerId));
    } catch (WorkerJobCancelledException e) {
      throw e;
    } catch (Exception e) {
      throw new WorkerException(String.format("Worker %s failed", workerId), e);
    } finally {
      taskRepository.fetch(workerTask).suspend();
      workerQueue.delete(workerId);
      workerStatusRepository.updateStatusFor(workerId, AVAILABLE);
      TaskReactorTelemetry.addWorkerWorkingTimeEvent(workStartTime, Instant.now());
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
