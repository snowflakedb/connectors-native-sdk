/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import static java.lang.String.format;

import com.snowflake.connectors.taskreactor.commands.processor.executors.CommandExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation of {@link CommandsProcessor} used by the Task Reactor. */
public class DefaultCommandsProcessor implements CommandsProcessor {

  private final Logger logger = LoggerFactory.getLogger(DefaultCommandsProcessor.class);
  private final CommandsQueueRepository commandsQueueRepository;
  private final ExecutorStrategies executorsStrategy;

  /**
   * Creates new object of {@link DefaultCommandsProcessor} with all fields initialized.
   *
   * @param commandsQueueRepository implementation of the {@link CommandsQueueRepository}
   * @param executorsStrategy implementation of the {@link ExecutorStrategies}
   */
  DefaultCommandsProcessor(
      CommandsQueueRepository commandsQueueRepository, ExecutorStrategies executorsStrategy) {
    this.commandsQueueRepository = commandsQueueRepository;
    this.executorsStrategy = executorsStrategy;
  }

  /**
   * Fetches all pending, valid commands from the commands queue and executes them using proper
   * {@link CommandExecutor} defined by the implementation of the {@link ExecutorStrategies}.
   * Deletes all unsupported commands from the queue at the end.
   */
  @Override
  public void processCommands() {
    var commands = commandsQueueRepository.fetchAllSupportedOrderedBySeqNo();

    logger.info(
        format(
            "Starting executing %s%n commands of types [%s]",
            commands.size(),
            commands.stream()
                .map(Command::getType)
                .map(Command.CommandType::name)
                .collect(Collectors.joining(", "))));
    commands.forEach(this::executeCommand);
    commandsQueueRepository.deleteUnsupportedCommands();
  }

  /**
   * Executes provided command with the proper command executor and removes the command from the
   * queue once the execution is finished regardless the execution result
   *
   * @param command command to be executed
   */
  private void executeCommand(Command command) {
    CommandExecutor commandExecutor =
        executorsStrategy
            .getStrategy(command.getType())
            .orElseThrow(
                () -> new CommandTypeUnsupportedByCommandsExecutorException(command.getType()));
    try {
      commandExecutor.execute(command);
      logger.info(
          format(
              "Command of type [%s] with id [%s] successfully executed.",
              command.getType().name(), command.getId()));
    } catch (Exception e) {
      logger.error(
          format(
              "Command of type [%s] with id [%s] execution failed. Cause: [%s]",
              command.getType().name(), command.getId(), e.getMessage()));
    }
    commandsQueueRepository.deleteById(command.getId());
  }
}
