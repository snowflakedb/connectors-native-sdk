/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.registry;

import static com.snowflake.connectors.util.sql.SnowparkFunctions.lit;
import static com.snowflake.snowpark_java.Functions.col;
import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Default implementation of {@link InstanceRegistryRepository} repository. */
class DefaultInstanceRegistryRepository implements InstanceRegistryRepository {

  private static final String TABLE_NAME = "TASK_REACTOR_INSTANCES.INSTANCE_REGISTRY";
  private static final String INSTANCE_NAME = "INSTANCE_NAME";
  private static final String IS_INITIALIZED = "IS_INITIALIZED";
  private static final String IS_ACTIVE = "IS_ACTIVE";

  private final Session session;

  DefaultInstanceRegistryRepository(Session session) {
    this.session = session;
  }

  @Override
  public List<TaskReactorInstance> fetchAll() {
    Row[] collect =
        session.table(TABLE_NAME).select(INSTANCE_NAME, IS_INITIALIZED, IS_ACTIVE).collect();
    return Arrays.stream(collect).map(this::mapToInstance).collect(toList());
  }

  @Override
  public List<TaskReactorInstance> fetchAllInitialized() {
    Row[] collect =
        session
            .table(TABLE_NAME)
            .select(INSTANCE_NAME, IS_INITIALIZED, IS_ACTIVE)
            .where(col(IS_INITIALIZED).equal_to(lit(true)))
            .collect();
    return Arrays.stream(collect).map(this::mapToInstance).collect(toList());
  }

  @Override
  public TaskReactorInstance fetch(Identifier instance) {
    Column instanceEqualsCondition = col(INSTANCE_NAME).equal_to(lit(instance.getValue()));
    Row[] result =
        session
            .table(TABLE_NAME)
            .select(INSTANCE_NAME, IS_INITIALIZED, IS_ACTIVE)
            .where(instanceEqualsCondition)
            .collect();
    return mapToInstance(result[0]);
  }

  @Override
  public void setInactive(Identifier instance) {
    setIsActive(instance, false);
  }

  @Override
  public void setActive(Identifier instance) {
    setIsActive(instance, true);
  }

  private void setIsActive(Identifier instance, boolean isActive) {
    Map<Column, Column> assignments = Map.of(col(IS_ACTIVE), lit(isActive));
    Column instanceEqualsCondition = col(INSTANCE_NAME).equal_to(lit(instance.getValue()));

    session.table(TABLE_NAME).update(assignments, instanceEqualsCondition);
  }

  private TaskReactorInstance mapToInstance(Row row) {
    return new TaskReactorInstance(
        Identifier.from(row.getString(0)), row.getBoolean(1), row.getBoolean(2));
  }
}
