/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.task;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.common.object.ObjectName;
import java.util.Map;

/** In memory implementation of {@link TaskRef}. */
public class InMemoryTaskRef implements TaskRef {

  private final ObjectName taskName;
  private final Map<ObjectName, TaskProperties> store;

  public InMemoryTaskRef(ObjectName taskName, Map<ObjectName, TaskProperties> store) {
    this.taskName = taskName;
    this.store = store;
  }

  @Override
  public ObjectName name() {
    return taskName;
  }

  @Override
  public void execute() {}

  @Override
  public boolean checkIfExists() {
    return store.containsKey(taskName);
  }

  @Override
  public void resume() {
    throwIfTaskDoesNotExist();
    resumeIfExists();
  }

  @Override
  public void resumeIfExists() {
    store.computeIfPresent(
        taskName,
        (taskName, taskProperties) ->
            new TaskProperties.Builder(taskProperties).withState("started").build());
  }

  @Override
  public void suspend() {
    throwIfTaskDoesNotExist();
    suspendIfExists();
  }

  @Override
  public void suspendIfExists() {
    store.computeIfPresent(
        taskName,
        (taskName, taskProperties) ->
            new TaskProperties.Builder(taskProperties).withState("suspended").build());
  }

  @Override
  public void drop() {
    throwIfTaskDoesNotExist();
    dropIfExists();
  }

  @Override
  public void dropIfExists() {
    store.remove(taskName);
  }

  @Override
  public void alterSchedule(String schedule) {
    throwIfTaskDoesNotExist();
    alterScheduleIfExists(schedule);
  }

  @Override
  public void alterScheduleIfExists(String schedule) {
    store.computeIfPresent(
        taskName,
        (taskName, properties) ->
            new TaskProperties.Builder(properties).withSchedule(schedule).build());
  }

  @Override
  public void alterWarehouse(String warehouse) {
    throwIfTaskDoesNotExist();
    alterWarehouseIfExists(warehouse);
  }

  @Override
  public void alterWarehouseIfExists(String warehouse) {
    store.computeIfPresent(
        taskName,
        (taskName, taskProperties) ->
            new TaskProperties.Builder(taskProperties)
                .withWarehouse(Identifier.fromWithAutoQuoting(warehouse))
                .build());
  }

  @Override
  public void grantMonitorPrivilegeToRole(String role) {}

  @Override
  public TaskProperties fetch() {
    return store.get(taskName);
  }

  private void throwIfTaskDoesNotExist() {
    if (store.get(taskName) == null) {
      throw new RuntimeException("Task does not exist");
    }
  }
}
