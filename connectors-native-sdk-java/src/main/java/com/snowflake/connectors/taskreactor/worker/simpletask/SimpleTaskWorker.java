/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.simpletask;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.TaskRepository;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.worker.Worker;
import com.snowflake.connectors.taskreactor.worker.WorkerId;
import com.snowflake.connectors.taskreactor.worker.queue.WorkItem;
import com.snowflake.connectors.taskreactor.worker.queue.WorkerQueue;
import com.snowflake.connectors.taskreactor.worker.status.WorkerStatusRepository;
import com.snowflake.snowpark_java.Session;

/** A worker executing a {@link SimpleTask}. */
public class SimpleTaskWorker<T> extends Worker<T> {

  private final SimpleTask<T> task;

  /**
   * Creates a new {@link SimpleTaskWorker}.
   *
   * @param session Snowpark session object
   * @param task task
   * @param workerId worker id
   * @param instanceSchema Task Reactor instance name
   * @param <T> task response type
   * @return a new simple task worker
   */
  public static <T> SimpleTaskWorker<T> from(
      Session session, SimpleTask<T> task, WorkerId workerId, Identifier instanceSchema) {
    return new SimpleTaskWorker<>(
        task,
        workerId,
        instanceSchema,
        WorkerStatusRepository.getInstance(session, instanceSchema),
        ObjectName.from(instanceSchema, ComponentNames.workerTask(workerId)),
        WorkerQueue.getInstance(session, instanceSchema),
        TaskRepository.getInstance(session));
  }

  SimpleTaskWorker(
      SimpleTask<T> task,
      WorkerId workerId,
      Identifier instanceName,
      WorkerStatusRepository workerStatusRepository,
      ObjectName workerTask,
      WorkerQueue workerQueue,
      TaskRepository taskRepository) {
    super(workerId, instanceName, workerStatusRepository, workerTask, workerQueue, taskRepository);
    this.task = task;
  }

  @Override
  protected T performWork(WorkItem workItem) {
    return task.execute(workItem);
  }
}
