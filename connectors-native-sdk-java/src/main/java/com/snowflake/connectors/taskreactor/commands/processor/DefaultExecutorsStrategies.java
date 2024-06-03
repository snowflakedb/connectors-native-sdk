/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.CANCEL_ONGOING_EXECUTIONS;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.SET_WORKERS_NUMBER;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.UPDATE_WAREHOUSE;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.processor.executors.CancelOngoingExecutionsExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.CommandExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.PauseInstanceExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.ResumeInstanceExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.SetWorkersNumberExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.UpdateWarehouseExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.snowpark_java.Session;
import java.util.Map;
import java.util.Optional;

/** Default implementation of the {@link ExecutorStrategies} used by the Task Reactor. */
class DefaultExecutorsStrategies implements ExecutorStrategies {

  private final Map<CommandType, CommandExecutor> executorStrategies;

  DefaultExecutorsStrategies(Session session, Identifier instanceName) {
    this.executorStrategies = initializeStrategies(session, instanceName);
  }

  /** Commands execution strategy */
  private Map<CommandType, CommandExecutor> initializeStrategies(
      Session session, Identifier instanceName) {
    return Map.of(
        PAUSE_INSTANCE, PauseInstanceExecutor.getInstance(session, instanceName),
        RESUME_INSTANCE, ResumeInstanceExecutor.getInstance(session, instanceName),
        UPDATE_WAREHOUSE, UpdateWarehouseExecutor.getInstance(session, instanceName),
        SET_WORKERS_NUMBER, new SetWorkersNumberExecutor(),
        CANCEL_ONGOING_EXECUTIONS, new CancelOngoingExecutionsExecutor());
  }

  /**
   * {@inheritDoc}
   *
   * @param commandType type of the command for which the proper executor should be selected
   * @return default command executor for the given command type
   */
  @Override
  public Optional<CommandExecutor> getStrategy(CommandType commandType) {
    return Optional.ofNullable(executorStrategies.get(commandType));
  }
}
