/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.ObjectName;
import java.util.HashMap;
import java.util.Map;

/** In memory implementation of {@link TaskRepository}. */
public class InMemoryTaskRepository implements TaskRepository {

  private final Map<ObjectName, TaskProperties> store = new HashMap<>();

  @Override
  public TaskRef create(TaskDefinition definition, boolean replace, boolean ifNotExists) {
    TaskProperties properties = definition.properties();
    store.put(properties.name(), properties);
    return new InMemoryTaskRef(properties.name(), store);
  }

  @Override
  public TaskRef fetch(ObjectName objectName) {
    return new InMemoryTaskRef(objectName, store);
  }

  public void clear() {
    store.clear();
  }
}
