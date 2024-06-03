/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.queue;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;

/** Representation of the command entity from the command queue table. */
public class Command {

  private final String id;
  private final CommandType type;
  private final Variant payload;
  private final long seqNo;

  /**
   * Creates a {@link Command} object with all fields initialized.
   *
   * @param id id of the commands
   * @param commandType the name of the command type which is mapped to one of {@link CommandType}
   *     values
   * @param payload custom optional payload that should match the contract defined by the command
   *     type
   * @param seqNo the value by which the commands execution order is defined
   */
  public Command(String id, String commandType, Variant payload, long seqNo) {
    this.id = id;
    this.type = validateCommandType(commandType);
    this.payload = payload;
    this.seqNo = seqNo;
  }

  /**
   * Returns the command id.
   *
   * @return id of the command
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the command type.
   *
   * @return command type
   */
  public CommandType getType() {
    return type;
  }

  /**
   * Returns the custom payload.
   *
   * @return command custom payload
   */
  public Variant getPayload() {
    return payload;
  }

  /**
   * Returns the sequence number.
   *
   * @return command sequence number
   */
  public long getSeqNo() {
    return seqNo;
  }

  /**
   * Validates whether the argument can be represented by one of {@link CommandType} values.
   *
   * @param commandType the name of the command type
   * @return one of the valid {@link CommandType} values mapped from the method argument
   * @throws InvalidCommandTypeException if the provided command type is not supported
   */
  private CommandType validateCommandType(String commandType) {
    return Arrays.stream(CommandType.values())
        .map(CommandType::name)
        .filter(type -> type.equals(commandType))
        .findFirst()
        .map(it -> CommandType.valueOf(it.toUpperCase()))
        .orElseThrow(() -> new InvalidCommandTypeException(commandType, this.id));
  }

  /** Valid command types that are recognized and supported by the command processor. */
  public enum CommandType {
    RESUME_INSTANCE,
    PAUSE_INSTANCE,
    UPDATE_WAREHOUSE,
    SET_WORKERS_NUMBER,
    CANCEL_ONGOING_EXECUTIONS;

    /**
     * @param commandType the type of the command as a literal to be validated whether is supported
     * @return boolean with information whether the passed type is valid or not
     */
    public static boolean isValidType(String commandType) {
      return Arrays.stream(values()).anyMatch(type -> type.name().equalsIgnoreCase(commandType));
    }
  }
}
