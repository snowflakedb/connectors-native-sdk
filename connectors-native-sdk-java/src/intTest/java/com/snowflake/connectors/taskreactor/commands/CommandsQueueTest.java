/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands;

import static com.snowflake.connectors.taskreactor.ComponentNames.COMMANDS_QUEUE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.PAUSE_INSTANCE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.RESUME_INSTANCE;
import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.SET_WORKERS_NUMBER;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.PAYLOAD;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.SEQ_NO;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.TYPE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.BaseTaskReactorIntegrationTest;
import com.snowflake.connectors.taskreactor.commands.queue.Command;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue;
import com.snowflake.connectors.taskreactor.utils.TaskReactorTestInstance;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CommandsQueueTest extends BaseTaskReactorIntegrationTest {

  private static final Variant EXAMPLE_COMMAND_PAYLOAD = new Variant(Map.of("key", "value"));

  private CommandsQueue repository;
  private TaskReactorTestInstance instance;

  @BeforeAll
  void beforeAll() {
    repository = CommandsQueue.getInstance(session, Identifier.from(TEST_INSTANCE));
    instance =
        TaskReactorTestInstance.buildFromScratch(TEST_INSTANCE, session)
            .withCommandsQueue()
            .createInstance();
  }

  @AfterAll
  void dropInstance() {
    instance.delete();
  }

  @AfterEach
  void cleanUp() {
    session.table(ObjectName.from(TEST_INSTANCE, COMMANDS_QUEUE).getValue()).delete();
  }

  @Test
  void shouldFetchCommandsOfValidTypeOrderedBySeqNo() {
    // given
    String firstInsertedCommandId = "1";
    long firstInsertedCommandSeqNo = 3L;

    String secondInsertedCommandId = "2";
    long secondInsertedCommandSeqNo = 2L;

    String thirdInsertedCommandId = "3";
    long thirdInsertedCommandSeqNo = 1L;

    insertCommand(
        firstInsertedCommandId,
        PAUSE_INSTANCE.name(),
        EXAMPLE_COMMAND_PAYLOAD,
        firstInsertedCommandSeqNo);
    insertCommand(
        secondInsertedCommandId,
        RESUME_INSTANCE.name(),
        EXAMPLE_COMMAND_PAYLOAD,
        secondInsertedCommandSeqNo);
    insertCommand(
        thirdInsertedCommandId,
        SET_WORKERS_NUMBER.name(),
        EXAMPLE_COMMAND_PAYLOAD,
        thirdInsertedCommandSeqNo);

    // when
    var result = repository.fetchAllSupportedOrderedBySeqNo();

    // then
    assertCommand(
        result.get(0),
        thirdInsertedCommandId,
        SET_WORKERS_NUMBER,
        EXAMPLE_COMMAND_PAYLOAD,
        thirdInsertedCommandSeqNo);
    assertCommand(
        result.get(1),
        secondInsertedCommandId,
        RESUME_INSTANCE,
        EXAMPLE_COMMAND_PAYLOAD,
        secondInsertedCommandSeqNo);
    assertCommand(
        result.get(2),
        firstInsertedCommandId,
        PAUSE_INSTANCE,
        EXAMPLE_COMMAND_PAYLOAD,
        firstInsertedCommandSeqNo);
  }

  @Test
  void shouldInsertNewCommand() {
    // given
    var commandType = PAUSE_INSTANCE;

    // when
    repository.add(commandType, EXAMPLE_COMMAND_PAYLOAD);

    // then
    var result = fetchFirstCommands();
    assertThat(result.getString(0)).isEqualTo(commandType.name());
    assertThat(result.getVariant(1)).isEqualTo(EXAMPLE_COMMAND_PAYLOAD);
    assertThat(result.getLong(2)).isEqualTo(1);
  }

  @Test
  void shouldDeleteCommandById() {
    // given
    var commandId = "example-id";
    insertCommand(commandId, PAUSE_INSTANCE.name(), EXAMPLE_COMMAND_PAYLOAD, 1);

    // when
    repository.deleteById(commandId);

    // then
    var commandsNumber = fetchAllCommands().length;
    assertThat(commandsNumber).isEqualTo(0);
  }

  @Test
  void shouldDeleteAllCommandsOfUnsupportedCommandType() {
    // given
    insertCommand("1", "invalidCommandType", EXAMPLE_COMMAND_PAYLOAD, 1);
    insertCommand("2", "invalidCommandType", EXAMPLE_COMMAND_PAYLOAD, 2);
    insertCommand("3", PAUSE_INSTANCE.name(), EXAMPLE_COMMAND_PAYLOAD, 3);

    // when
    repository.deleteUnsupportedCommands();

    // then
    assertThat(fetchAllCommands().length).isEqualTo(1);
  }

  private void assertCommand(
      Command command, String id, CommandType type, Variant payload, long seqNo) {
    assertThat(command.getId()).isEqualTo(id);
    assertThat(command.getType()).isEqualTo(type);
    assertThat(command.getPayload()).isEqualTo(payload);
    assertThat(command.getSeqNo()).isEqualTo(seqNo);
  }

  private Row[] fetchAllCommands() {
    return session
        .table(ObjectName.from(TEST_INSTANCE, COMMANDS_QUEUE).getValue())
        .select(TYPE, PAYLOAD, SEQ_NO)
        .collect();
  }

  private Row fetchFirstCommands() {
    return fetchAllCommands()[0];
  }

  private void insertCommand(String id, String type, Variant payload, long seqNo) {
    session
        .sql(
            format(
                "INSERT INTO %s SELECT '%s', '%s', PARSE_JSON('%s'), %s%n",
                ObjectName.from(TEST_INSTANCE, COMMANDS_QUEUE).getValue(),
                id,
                type,
                payload.asJsonString(),
                seqNo))
        .collect();
  }
}
