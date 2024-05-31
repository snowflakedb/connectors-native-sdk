/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.utils;

public class Constants {
  public static final String TASK_REACTOR_API_SCHEMA = "TASK_REACTOR";
  public static final String CREATE_INSTANCE_SCHEMA_PROCEDURE = "CREATE_INSTANCE_SCHEMA";
  public static final String CREATE_QUEUE_PROCEDURE = "CREATE_QUEUE";
  public static final String CREATE_WORKER_REGISTRY_SEQUENCE_PROCEDURE =
      "CREATE_WORKER_REGISTRY_SEQUENCE";
  public static final String CREATE_WORKER_REGISTRY_PROCEDURE = "CREATE_WORKER_REGISTRY";
  public static final String CREATE_WORKER_STATUS_PROCEDURE = "CREATE_WORKER_STATUS_TABLE";
  public static final String CREATE_CONFIG_PROCEDURE = "CREATE_CONFIG_TABLE";
  public static final String CREATE_COMMANDS_QUEUE_PROCEDURE = "CREATE_COMMANDS_QUEUE";
  public static final String QUEUE = "QUEUE";
  public static final String QUEUE_STREAM = "QUEUE_STREAM";
  public static final String COMMANDS_QUEUE = "COMMANDS_QUEUE";
  public static final String COMMANDS_QUEUE_STREAM = "COMMANDS_QUEUE_STREAM";
  public static final String COMMANDS_QUEUE_SEQUENCE = "COMMANDS_QUEUE_SEQUENCE";
  public static final String WORKER_REGISTRY = "WORKER_REGISTRY";
  public static final String WORKER_REGISTRY_SEQUENCE = "WORKER_REGISTRY_SEQUENCE";
  public static final String WORKER_STATUS = "WORKER_STATUS";
  public static final String CONFIG = "CONFIG";
  public static final String TASK_REACTOR_INSTANCES_SCHEMA_NAME = "TASK_REACTOR_INSTANCES";
  public static final String INSTANCE_REGISTRY = "INSTANCE_REGISTRY";

  public static enum WorkSelectorType {
    PROCEDURE,
    VIEW
  }
}
