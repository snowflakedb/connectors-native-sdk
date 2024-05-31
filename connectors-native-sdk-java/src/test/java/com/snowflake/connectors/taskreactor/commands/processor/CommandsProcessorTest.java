/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.processor;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.connectors.taskreactor.commands.processor.executors.CommandExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.PauseInstanceExecutor;
import com.snowflake.connectors.taskreactor.commands.processor.executors.ResumeInstanceExecutor;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueueRepository;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class CommandsProcessorTest {

  @Test
  void shouldProcessFetchedCommandsAndDeleteUnsupported() {
    // given
    String pauseInstanceCommandId = "pauseCommand";
    Command pauseInstanceCommand =
        new Command(pauseInstanceCommandId, PAUSE_INSTANCE.name(), new Variant(""), 3);
    String resumeInstanceCommandId = "resumeCommandId";
    Command resumeInstanceCommand =
        new Command(resumeInstanceCommandId, RESUME_INSTANCE.name(), new Variant(""), 1);

    CommandExecutor pauseInstanceExecutor = mock(PauseInstanceExecutor.class);
    doNothing().when(pauseInstanceExecutor).execute(pauseInstanceCommand);
    CommandExecutor resumeInstanceExecutor = mock(ResumeInstanceExecutor.class);
    doNothing().when(resumeInstanceExecutor).execute(resumeInstanceCommand);

    ExecutorStrategies executorsStrategy = mock(ExecutorStrategies.class);
    when(executorsStrategy.getStrategy(PAUSE_INSTANCE))
        .thenReturn(Optional.of(pauseInstanceExecutor));
    when(executorsStrategy.getStrategy(RESUME_INSTANCE))
        .thenReturn(Optional.of(resumeInstanceExecutor));

    CommandsQueueRepository commandsQueueRepository = mock(CommandsQueueRepository.class);
    when(commandsQueueRepository.fetchAllSupportedOrderedBySeqNo())
        .thenReturn(List.of(resumeInstanceCommand, pauseInstanceCommand));
    var commandsProcessor =
        new DefaultCommandsProcessor(commandsQueueRepository, executorsStrategy);

    // when
    commandsProcessor.processCommands();

    // then
    InOrder inOrder =
        inOrder(pauseInstanceExecutor, resumeInstanceExecutor, commandsQueueRepository);
    inOrder.verify(resumeInstanceExecutor).execute(resumeInstanceCommand);
    inOrder.verify(commandsQueueRepository).deleteById(resumeInstanceCommandId);
    inOrder.verify(pauseInstanceExecutor).execute(pauseInstanceCommand);
    inOrder.verify(commandsQueueRepository).deleteById(pauseInstanceCommandId);
    inOrder.verify(commandsQueueRepository).deleteUnsupportedCommands();
  }

  @Test
  void shouldThrowWhenNoneOfCommandExecutorsSupportsGivenCommandType() {
    // given
    Command pauseInstanceCommand = new Command("", PAUSE_INSTANCE.name(), new Variant(""), 3);

    ExecutorStrategies executorsStrategy = mock(ExecutorStrategies.class);
    when(executorsStrategy.getStrategy(PAUSE_INSTANCE)).thenReturn(Optional.ofNullable(null));

    CommandsQueueRepository commandsQueueRepository = mock(CommandsQueueRepository.class);
    when(commandsQueueRepository.fetchAllSupportedOrderedBySeqNo())
        .thenReturn(List.of(pauseInstanceCommand));
    var commandsProcessor =
        new DefaultCommandsProcessor(commandsQueueRepository, executorsStrategy);

    // expect
    assertThatExceptionOfType(CommandTypeUnsupportedByCommandsExecutorException.class)
        .isThrownBy(commandsProcessor::processCommands)
        .withMessage(
            format(
                "Command of type [%s] is not supported by the Commands Processor.",
                PAUSE_INSTANCE.name()));
  }
}
