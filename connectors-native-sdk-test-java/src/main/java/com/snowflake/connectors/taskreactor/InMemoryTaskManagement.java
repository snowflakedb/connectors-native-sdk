/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor;

import static com.snowflake.connectors.taskreactor.ComponentNames.TASK_REACTOR_SCHEMA;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.task.InMemoryTaskRef;
import com.snowflake.connectors.common.task.TaskDefinition;
import com.snowflake.connectors.common.task.TaskLister;
import com.snowflake.connectors.common.task.TaskProperties;
import com.snowflake.connectors.common.task.TaskRef;
import com.snowflake.connectors.common.task.TaskRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** In memory implementation of {@link TaskRepository} and {@link TaskLister}. */
public class InMemoryTaskManagement
    implements TaskRepository, TaskLister, TaskReactorExistenceVerifier {

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

  /** Clears the task storage of this object. */
  public void clear() {
    store.clear();
  }

  @Override
  public Optional<TaskProperties> showTask(ObjectName taskName) {
    var foundTasks = showTasks(getSchemaValue(taskName), taskName.getName().getValue());
    return foundTasks.stream().findFirst();
  }

  @Override
  public List<TaskProperties> showTasks(String schema) {
    return store.entrySet().stream()
        .filter(task -> Objects.equals(getSchemaValue(task.getKey()), schema))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<TaskProperties> showTasks(String schema, String like) {
    return store.entrySet().stream()
        .filter(
            task ->
                Objects.equals(getSchemaValue(task.getKey()), schema)
                    && task.getKey().getName().getValue().contains(like))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Returns all tasks stored in this object.
   *
   * @return all tasks stored in this object
   */
  public Map<ObjectName, TaskProperties> fetchAll() {
    return store;
  }

  @Override
  public boolean isTaskReactorConfigured() {
    return store.keySet().stream()
        .map(this::getSchemaValue)
        .anyMatch(it -> Objects.equals(it, TASK_REACTOR_SCHEMA));
  }

  private String getSchemaValue(ObjectName objectName) {
    return objectName.getSchema().map(Identifier::getValue).orElse(null);
  }
}
