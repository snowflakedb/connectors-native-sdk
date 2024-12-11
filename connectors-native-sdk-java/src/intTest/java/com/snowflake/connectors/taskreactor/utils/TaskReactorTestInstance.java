/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

import static com.snowflake.connectors.taskreactor.utils.Constants.COMMANDS_QUEUE;
import static com.snowflake.connectors.taskreactor.utils.Constants.COMMANDS_QUEUE_SEQUENCE;
import static com.snowflake.connectors.taskreactor.utils.Constants.COMMANDS_QUEUE_STREAM;
import static com.snowflake.connectors.taskreactor.utils.Constants.CONFIG;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_COMMANDS_QUEUE_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_CONFIG_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_INSTANCE_SCHEMA_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_QUEUE_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_WORKER_REGISTRY_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_WORKER_REGISTRY_SEQUENCE_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.CREATE_WORKER_STATUS_PROCEDURE;
import static com.snowflake.connectors.taskreactor.utils.Constants.INSTANCE_REGISTRY;
import static com.snowflake.connectors.taskreactor.utils.Constants.QUEUE;
import static com.snowflake.connectors.taskreactor.utils.Constants.QUEUE_STREAM;
import static com.snowflake.connectors.taskreactor.utils.Constants.TASK_REACTOR_API_SCHEMA;
import static com.snowflake.connectors.taskreactor.utils.Constants.TASK_REACTOR_INSTANCES_SCHEMA_NAME;
import static com.snowflake.connectors.taskreactor.utils.Constants.WORKER_REGISTRY;
import static com.snowflake.connectors.taskreactor.utils.Constants.WORKER_REGISTRY_SEQUENCE;
import static com.snowflake.connectors.taskreactor.utils.Constants.WORKER_STATUS;
import static com.snowflake.connectors.taskreactor.utils.SqlHelper.prepareProcedureCallStatement;
import static com.snowflake.connectors.taskreactor.utils.SqlHelper.varcharArg;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.taskreactor.ComponentNames;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.List;

public class TaskReactorTestInstance {

  private final String name;
  private final TaskReactorInstanceConfiguration configuration;
  private final Session session;
  private final List<String> queries;

  private TaskReactorTestInstance(
      String instanceName,
      TaskReactorInstanceConfiguration configuration,
      Session session,
      List<String> queries) {
    this.name = instanceName;
    this.configuration = configuration;
    this.session = session;
    this.queries = queries;
    createRequiredInstanceObjects();
    registerInstance();
  }

  public static InstanceManager buildFromScratch(String instanceName, Session session) {
    return new InstanceManager(instanceName, session);
  }

  public String getName() {
    return name;
  }

  public void delete() {
    session.sql(format("DROP SCHEMA IF EXISTS %s", name)).collect();
    session
        .sql(
            format(
                "DELETE FROM %s.%s where INSTANCE_NAME = '%s'",
                TASK_REACTOR_INSTANCES_SCHEMA_NAME, INSTANCE_REGISTRY, name.toUpperCase()))
        .collect();
  }

  public void setFakeIsActive(boolean isActive) {
    session
        .sql(
            format(
                "UPDATE %s.%s SET IS_ACTIVE = %s WHERE INSTANCE_NAME = '%s'",
                TASK_REACTOR_INSTANCES_SCHEMA_NAME, INSTANCE_REGISTRY, isActive, this.name))
        .collect();
  }

  public void updateConfig(String key, String value) {
    session
        .sql(
            format(
                "UPDATE %s SET VALUE = '%s' WHERE KEY = '%s'",
                ObjectName.from(this.name, ComponentNames.CONFIG_TABLE).getValue(), value, key))
        .collect();
  }

  private void createRequiredInstanceObjects() {
    queries.forEach(query -> session.sql(query).collect());
  }

  private void registerInstance() {
    session
        .sql(
            format(
                "INSERT INTO %s.%s (INSTANCE_NAME) VALUES ('%s')",
                TASK_REACTOR_INSTANCES_SCHEMA_NAME, INSTANCE_REGISTRY, this.name))
        .collect();
  }

  public static class InstanceManager {
    private final String instanceName;
    private TaskReactorInstanceConfiguration configuration;
    private final Session session;
    private final List<String> queries = new ArrayList<>();

    public InstanceManager(String instanceName, Session session) {
      this.session = session;
      this.instanceName = instanceName;
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA, CREATE_INSTANCE_SCHEMA_PROCEDURE, varcharArg(instanceName)));
    }

    public TaskReactorTestInstance createInstance() {
      return new TaskReactorTestInstance(
          requireNonNull(this.instanceName),
          configuration,
          requireNonNull(this.session),
          this.queries);
    }

    public InstanceManager withQueue() {
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_QUEUE_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(QUEUE),
              varcharArg(QUEUE_STREAM)));
      return this;
    }

    public InstanceManager withWorkerRegistry() {
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_WORKER_REGISTRY_SEQUENCE_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(WORKER_REGISTRY_SEQUENCE)));
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_WORKER_REGISTRY_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(WORKER_REGISTRY),
              varcharArg(WORKER_REGISTRY_SEQUENCE)));
      return this;
    }

    public InstanceManager withWorkerStatus() {
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_WORKER_STATUS_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(WORKER_STATUS)));
      return this;
    }

    public InstanceManager withConfig(TaskReactorInstanceConfiguration configuration) {
      this.configuration = configuration;
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_CONFIG_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(CONFIG),
              varcharArg(configuration.getWorkerProcedureName()),
              varcharArg(configuration.getWorkSelectorType().name()),
              varcharArg(configuration.getWorkSelectorName()),
              varcharArg(configuration.getExpiredWorkSelector()),
              "false"));
      return this;
    }

    public InstanceManager withCommandsQueue() {
      queries.add(
          prepareProcedureCallStatement(
              TASK_REACTOR_API_SCHEMA,
              CREATE_COMMANDS_QUEUE_PROCEDURE,
              varcharArg(instanceName),
              varcharArg(COMMANDS_QUEUE),
              varcharArg(COMMANDS_QUEUE_SEQUENCE),
              varcharArg(COMMANDS_QUEUE_STREAM)));
      return this;
    }
  }
}
