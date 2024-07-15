/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.lifecycle;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.dispatcher.DispatcherTaskManager;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;

/**
 * Service which is used to start the process of resuming the Task Reactor which was paused. It
 * inserts RESUME_INSTANCE command into command queue and resumes the dispatcher's task. Then the
 * Task Reactor instances are actually resumed by dispatcher which handles the command.
 */
public class ResumeTaskReactorService {

  private static final Logger LOG = TaskReactorLogger.getLogger(ResumeTaskReactorService.class);

  private final TaskReactorInstanceComponentProvider componentProvider;
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor;

  /**
   * Creates a new instance of the ResumeTaskReactorService.
   *
   * @param session Snowpark session object
   * @return a new ResumeTaskReactorService instance
   */
  public static ResumeTaskReactorService getInstance(Session session) {
    return new ResumeTaskReactorService(
        TaskReactorInstanceComponentProvider.getInstance(session),
        TaskReactorInstanceActionExecutor.getInstance(session));
  }

  ResumeTaskReactorService(
      TaskReactorInstanceComponentProvider componentProvider,
      TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor) {
    this.componentProvider = componentProvider;
    this.taskReactorInstanceActionExecutor = taskReactorInstanceActionExecutor;
  }

  /**
   * Resumes a given Task Reactor instance
   *
   * @param instanceSchema name of the Task Reactor instance to be resumed
   */
  public void resumeInstance(Identifier instanceSchema) {
    LOG.info("Started resuming Task Reactor instance: {}", instanceSchema);
    CommandsQueueRepository commandsQueueRepository =
        componentProvider.commandsQueueRepository(instanceSchema);
    DispatcherTaskManager dispatcherTaskManager =
        componentProvider.dispatcherTaskManager(instanceSchema);

    commandsQueueRepository.addCommandWithEmptyPayload(RESUME_INSTANCE);
    dispatcherTaskManager.resumeDispatcherTask();
    LOG.info("Added RESUME_INSTANCE command to the command queue (instance: {})", instanceSchema);
  }

  /** Resumes all Task Reactor instances defined in Instance Registry */
  public void resumeAllInstances() {
    taskReactorInstanceActionExecutor.applyToAllInitializedTaskReactorInstances(
        this::resumeInstance);
  }
}
