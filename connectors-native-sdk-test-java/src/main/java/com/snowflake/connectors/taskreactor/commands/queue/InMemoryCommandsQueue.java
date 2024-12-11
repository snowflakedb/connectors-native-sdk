/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.commands.queue;

import com.snowflake.connectors.taskreactor.commands.queue.Command.CommandType;
import com.snowflake.snowpark_java.types.Variant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** In memory implementation of {@link CommandsQueue}. */
public class InMemoryCommandsQueue implements CommandsQueue {

  private final List<Command> store = new ArrayList<>();

  @Override
  public List<Command> fetchAllSupportedOrderedBySeqNo() {
    return new ArrayList<>(store);
  }

  @Override
  public void add(CommandType type, Variant payload) {
    store.add(new Command(UUID.randomUUID().toString(), type.name(), payload, 1));
  }

  @Override
  public void deleteById(String id) {
    store.removeIf(command -> id.equals(command.getId()));
  }

  @Override
  public void deleteUnsupportedCommands() {}
}
