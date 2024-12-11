/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.queue;

import static com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType.isValidType;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.ID;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.PAYLOAD;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.SEQ_NO;
import static com.snowflake.connectors.taskreactor.commands.queue.CommandsQueue.ColumnName.TYPE;
import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.connectors.taskreactor.log.TaskReactorLogger;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/** Default implementation of the {@link CommandsQueue} used by the Task Reactor. */
public class DefaultCommandsQueue implements CommandsQueue {

  private static final Logger LOG = TaskReactorLogger.getLogger(DefaultCommandsQueue.class);

  private final Session session;
  private final ObjectName commandsQueueName;

  /**
   * Creates a {@link DefaultCommandsQueue} object with all fields initialized.
   *
   * @param session Snowpark session object
   * @param instanceSchema task reactor instance schema name
   */
  public DefaultCommandsQueue(Session session, Identifier instanceSchema) {
    this.session = session;
    this.commandsQueueName =
        ObjectName.from(instanceSchema, Identifier.from(ComponentNames.COMMANDS_QUEUE));
  }

  /**
   * {@inheritDoc} This implementation assumes that there can be some commands in the queue with the
   * invalid command type. These commands are filtered out from the queue.
   *
   * @return {@inheritDoc}
   */
  @Override
  public List<Command> fetchAllSupportedOrderedBySeqNo() {
    return Arrays.stream(
            session
                .table(commandsQueueName.getValue())
                .select(ID, TYPE, PAYLOAD, SEQ_NO)
                .sort(col(SEQ_NO))
                .collect())
        .filter(row -> isValidType(row.getString(1)))
        .map(this::mapToCommand)
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * @param type type of the command
   * @param payload custom command payload appropriate for the command type
   */
  @Override
  public void add(CommandType type, Variant payload) {
    var table = session.table(commandsQueueName.getValue());
    StructType schema =
        StructType.create(
            new StructField(TYPE, DataTypes.StringType),
            new StructField(PAYLOAD, DataTypes.VariantType));
    var source = session.createDataFrame(new Row[] {Row.create(type.name(), payload)}, schema);
    var assignments =
        Map.of(
            col(TYPE), source.col(TYPE),
            col(PAYLOAD), source.col(PAYLOAD));
    table
        .merge(source, source.col(TYPE).equal_to(table.col(ID)))
        .whenNotMatched()
        .insert(assignments)
        .collect();

    LOG.trace("Added new [{}] command with payload [{}].", type.name(), payload.asJsonString());
  }

  /**
   * {@inheritDoc}
   *
   * @param id id of the command entity to be deleted
   */
  @Override
  public void deleteById(String id) {
    session.table(commandsQueueName.getValue()).delete(col(ID).equal_to(lit(id)));
    LOG.trace("Deleted command with id [{}].", id);
  }

  /** Deletes all commands of type that is not included in the {@link CommandType} set of values */
  @Override
  public void deleteUnsupportedCommands() {
    Object[] supportedTypes = Arrays.stream(CommandType.values()).map(CommandType::name).toArray();
    session.table(commandsQueueName.getValue()).delete(col(TYPE).in(supportedTypes).unary_not());
  }

  /**
   * Maps the row representing command entity to {@link Command} object.
   *
   * @param row raw row fetched from the commands queue
   * @return the command entity mapped to java object
   */
  private Command mapToCommand(Row row) {
    return new Command(
        row.getString(0), row.getString(1).toUpperCase(), row.getVariant(2), row.getLong(3));
  }

  /**
   * Deletes the command entity from the commands queue table by its id when the command type is
   * recognized as invalid.
   *
   * @param id id of the command
   * @param type command type
   */
  private void deleteFromQueueIfInvalidType(String id, String type) {
    if (!isValidType(type)) {
      deleteById(id);
      LOG.trace(
          "Command with id [{}] removed from Commands Queue because type [{}] is invalid.",
          id,
          type);
    }
  }
}
