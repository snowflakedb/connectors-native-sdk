/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.queue;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/** Task Reactor repository that works with the commands queue table. */
public interface CommandsQueueRepository {

  Variant EMPTY_COMMAND_PAYLOAD = new Variant("{}");

  /**
   * Retrieves all commands of valid type from the commands queue sorted ascending by the command
   * sequence number.
   *
   * @return a list of the fetched commands
   */
  List<Command> fetchAllSupportedOrderedBySeqNo();

  /**
   * Creates a new command.
   *
   * @param type type of the command
   * @param payload custom command payload appropriate for the command type
   */
  void add(CommandType type, Variant payload);

  /**
   * Creates a new command with empty payload.
   *
   * @param type type of the command
   */
  default void addCommandWithEmptyPayload(CommandType type) {
    add(type, EMPTY_COMMAND_PAYLOAD);
  }

  /**
   * Deletes the command with the given id.
   *
   * @param id id of the command entity to be deleted
   */
  void deleteById(String id);

  /** Deletes all commands of type that should not be supported by the commands processor */
  void deleteUnsupportedCommands();

  /**
   * Returns a new instance of the {@link DefaultCommandsQueueRepository} which is default
   * implementation.
   *
   * @param session Snowpark session object
   * @param instanceName task reactor instance schema name
   * @return default, used by the task reactor implementation of the {@link CommandsQueueRepository}
   */
  static DefaultCommandsQueueRepository getInstance(Session session, Identifier instanceName) {
    return new DefaultCommandsQueueRepository(session, instanceName);
  }

  /** Column names of the commands queue table. */
  class ColumnName {

    /** Representation of the ID column. */
    public static final String ID = "ID";

    /** Representation of the TYPE column. */
    public static final String TYPE = "TYPE";

    /** Representation of the PAYLOAD column. */
    public static final String PAYLOAD = "PAYLOAD";

    /** Representation of the SEQ_NO column. */
    public static final String SEQ_NO = "SEQ_NO";
  }
}
