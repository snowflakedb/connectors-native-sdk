/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.lifecycle;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceActionExecutor;
import com.snowflake.connectors.taskreactor.TaskReactorInstanceComponentProvider;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;

/**
 * Service which is used to start the process of pausing the Task Reactor. It inserts PAUSE_INSTANCE
 * command into command queue. Then the Task Reactor instances are actually paused by dispatcher
 * which handles the command.
 */
public class PauseTaskReactorService {

  private static final Logger LOG = TaskReactorLogger.getLogger(PauseTaskReactorService.class);

  private final TaskReactorInstanceComponentProvider componentProvider;
  private final TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor;

  /**
   * Creates a new instance of the PauseTaskReactorService.
   *
   * @param session Snowpark session object
   * @return a new PauseTaskReactorService instance
   */
  public static PauseTaskReactorService getInstance(Session session) {
    return new PauseTaskReactorService(
        TaskReactorInstanceComponentProvider.getInstance(session),
        TaskReactorInstanceActionExecutor.getInstance(session));
  }

  PauseTaskReactorService(
      TaskReactorInstanceComponentProvider componentProvider,
      TaskReactorInstanceActionExecutor taskReactorInstanceActionExecutor) {
    this.componentProvider = componentProvider;
    this.taskReactorInstanceActionExecutor = taskReactorInstanceActionExecutor;
  }

  /**
   * Pauses a given Task Reactor instance
   *
   * @param instanceSchema name of the Task Reactor instance to be paused
   */
  public void pauseInstance(Identifier instanceSchema) {
    LOG.info("Started pausing Task Reactor instance: {}", instanceSchema);
    CommandsQueueRepository commandsQueueRepository =
        componentProvider.commandsQueueRepository(instanceSchema);
    commandsQueueRepository.addCommandWithEmptyPayload(PAUSE_INSTANCE);
    LOG.info("Added PAUSE_INSTANCE command to the command queue (instance: {})", instanceSchema);
  }

  /** Pauses all Task Reactor instances defined in Instance Registry */
  public void pauseAllInstances() {
    taskReactorInstanceActionExecutor.applyToAllInitializedTaskReactorInstances(
        this::pauseInstance);
  }
}
