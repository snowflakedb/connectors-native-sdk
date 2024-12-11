/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example;

import com.snowflake.connectors.example.configuration.connection.TemplateConnectionValidator;
import com.snowflake.connectors.example.configuration.finalize.TemplateFinalizeConnectorConfigurationCustomHandler;

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

  /**
   * Name of the connection testing procedure, backend implementation of which is provided by the
   * {@link TemplateConnectionValidator}.
   */
  public static final String TEST_CONNECTION_PROCEDURE = "TEST_CONNECTION";

  /**
   * Name of the configuration finalization procedure, backend implementation of which is provided
   * by the {@link TemplateFinalizeConnectorConfigurationCustomHandler}.
   */
  public static final String FINALIZE_CONNECTOR_CONFIGURATION_PROCEDURE =
      "FINALIZE_CONNECTOR_CONFIGURATION";

  /** Name of the worker procedure, used by the task reactor instance. */
  public static final String WORKER_PROCEDURE = "TEMPLATE_WORKER";

  /** Name of the procedure used for task reactor worker number setting. */
  public static final String SET_WORKERS_NUMBER_PROCEDURE = "SET_WORKERS_NUMBER";

  /**
   * Name of the procedure used for altering external source related procedures with references of
   * EAI and SECRET.
   */
  public static final String SETUP_EXTERNAL_INTEGRATION_WITH_REFS_PROCEDURE =
      "SETUP_EXTERNAL_INTEGRATION_WITH_REFERENCES";

  /**
   * Name of the procedure used for altering external source related procedures with names of EAI
   * and SECRET stored as connection configuration.
   */
  public static final String SETUP_EXTERNAL_INTEGRATION_WITH_NAMES_PROCEDURE =
      "SETUP_EXTERNAL_INTEGRATION_WITH_NAMES";

  /** Name of the internal connector schema. */
  public static final String STATE_SCHEMA = "STATE";

  /** Name of the main connector schema. */
  public static final String PUBLIC_SCHEMA = "PUBLIC";
}
