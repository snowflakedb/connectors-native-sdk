/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example;

/** Simple constant aggregation class for various connector object names. */
public class ConnectorObjects {

  /** Name of the table, where the ingested issues are stored. */
  public static final String DATA_TABLE = "DATA";

  /** Name of the view, which provides structured issue data from the {@link #DATA_TABLE}. */
  public static final String DATA_VIEW = DATA_TABLE + "_VIEW";

  /** Name of the task used by the scheduler system. */
  public static final String SCHEDULER_TASK = "SCHEDULER_TASK";

  /** Name of the task reactor instance used by the connector. */
  public static final String TASK_REACTOR_INSTANCE = "EXAMPLE_CONNECTOR_TASK_REACTOR";

  /** Name of the task reactor dispatcher task. */
  public static final String DISPATCHER_TASK = "DISPATCHER_TASK";

  /** Name of the task reactor API schema. */
  public static final String TASK_REACTOR_SCHEMA = "TASK_REACTOR";

  /** Name of the procedure used for task reactor instance initialization. */
  public static final String INITIALIZE_INSTANCE_PROCEDURE = "INITIALIZE_INSTANCE";

  /** Name of the procedure used for task reactor worker number setting. */
  public static final String SET_WORKERS_NUMBER_PROCEDURE = "SET_WORKERS_NUMBER";

  /** Name of the internal connector schema. */
  public static final String STATE_SCHEMA = "STATE";
}
