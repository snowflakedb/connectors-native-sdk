/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.DATA_TABLE;
import static com.snowflake.connectors.example.ConnectorObjects.DATA_VIEW;
import static com.snowflake.connectors.example.ConnectorObjects.INITIALIZE_INSTANCE_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.SET_WORKERS_NUMBER_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.TASK_REACTOR_INSTANCE;
import static com.snowflake.connectors.example.ConnectorObjects.TASK_REACTOR_SCHEMA;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.callProcedure;
import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.finalization.FinalizeConnectorCallback;
import com.snowflake.connectors.application.scheduler.SchedulerManager;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.object.Reference;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link FinalizeConnectorCallback}, used by the {@link
 * TemplateFinalizeConnectorConfigurationCustomHandler}, providing final connector configuration
 * logic:
 *
 * <ul>
 *   <li>destination schema and database setup
 *   <li>task reactor instance initialization
 *   <li>scheduler initialization
 * </ul>
 */
public class TemplateFinalizeConnectorConfigurationInternal implements FinalizeConnectorCallback {

  private static final Reference WAREHOUSE_REFERENCE = Reference.from("WAREHOUSE_REFERENCE");
  private static final String NULL_ARG = "null";

  private final Session session;
  private final SchedulerManager schedulerManager;

  public TemplateFinalizeConnectorConfigurationInternal(
      Session session, SchedulerManager schedulerManager) {
    this.session = session;
    this.schedulerManager = schedulerManager;
  }

  @Override
  public ConnectorResponse execute(Variant variant) {
    var connectorConfig = Configuration.fromConnectorConfig(session);
    var destinationDatabase = connectorConfig.getValue(DESTINATION_DATABASE.getPropertyName());
    var destinationSchema = connectorConfig.getValue(DESTINATION_SCHEMA.getPropertyName());

    if (destinationDatabase.isEmpty()) {
      return Configuration.keyNotFoundResponse(DESTINATION_DATABASE.getPropertyName());
    }

    if (destinationSchema.isEmpty()) {
      return Configuration.keyNotFoundResponse(DESTINATION_SCHEMA.getPropertyName());
    }

    var destinationTableName =
        ObjectName.from(destinationDatabase.get(), destinationSchema.get(), DATA_TABLE).getValue();
    var issuesView =
        ObjectName.from(destinationDatabase.get(), destinationSchema.get(), DATA_VIEW).getValue();

    createDestinationDbObjects(destinationDatabase.get(), destinationSchema.get());
    createSinkTable(destinationTableName);
    createSinkView(issuesView, destinationTableName);

    // Task reactor and scheduler components initialization. Those components handle work
    // distribution and scheduling for the ingestion of configured resources.
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/task_reactor_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/scheduler_reference
    initializeTaskReactorInstance();
    initializeScheduler();
    setTaskReactorWorkersNumber();

    // TODO: IMPLEMENT ME finalize internal: Implement any additional custom logic for finalization
    // of the connector. For example saving the config
    // See more in docs:
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/reference/finalize_configuration_reference
    // https://other-docs.snowflake.com/LIMITEDACCESS/connector-sdk/flow/finalize_configuration
    return ConnectorResponse.success(
        "This method needs to be implemented. Search for IMPLEMENT ME finalize internal");
  }

  private void createDestinationDbObjects(String destinationDatabase, String destinationSchema) {
    session.sql(format("CREATE DATABASE IF NOT EXISTS %s", destinationDatabase)).collect();
    session
        .sql(format("GRANT USAGE ON DATABASE %s TO APPLICATION ROLE ADMIN", destinationDatabase))
        .collect();
    session
        .sql(format("CREATE SCHEMA IF NOT EXISTS %s.%s", destinationDatabase, destinationSchema))
        .collect();
    session
        .sql(
            format(
                "GRANT USAGE ON SCHEMA %s.%s TO APPLICATION ROLE ADMIN",
                destinationDatabase, destinationSchema))
        .collect();
  }

  private void createSinkTable(String destinationTable) {
    // TODO: HINT: This implementation assumes a single sink table, that's why it is configured
    // here. If
    // data for each resource should be stored separately, then it's better to create the tables and
    // view when scheduling a resource or during ingestion if needed.
    // TODO: If data should be stored in some other way than just raw variant then it should be
    // customized here.
    session
        .sql(format("CREATE TABLE IF NOT EXISTS %s (RAW_DATA VARIANT)", destinationTable))
        .collect();
    session
        .sql(format("GRANT ALL ON TABLE %s TO APPLICATION ROLE ADMIN", destinationTable))
        .collect();
    session
        .sql(format("GRANT SELECT ON TABLE %s TO APPLICATION ROLE DATA_READER", destinationTable))
        .collect();
  }

  private void createSinkView(String issuesView, String destinationTable) {
    session
        .sql(
            format(
                "CREATE VIEW IF NOT EXISTS %s AS ( "
                    + " SELECT RAW_DATA:id as id,\n"
                    + " RAW_DATA:timestamp as timestamp,\n"
                    + " RAW_DATA:resource_id as resource_id,\n"
                    + " from %s "
                    + " )",
                issuesView, destinationTable))
        .collect();
    session.sql(format("GRANT SELECT ON VIEW %s TO APPLICATION ROLE ADMIN", issuesView)).collect();
    session
        .sql(format("GRANT SELECT ON VIEW %s TO APPLICATION ROLE DATA_READER", issuesView))
        .collect();
  }

  private void initializeTaskReactorInstance() {
    callProcedure(
        session,
        TASK_REACTOR_SCHEMA,
        INITIALIZE_INSTANCE_PROCEDURE,
        asVarchar(TASK_REACTOR_INSTANCE),
        asVarchar(WAREHOUSE_REFERENCE.getValue()),
        NULL_ARG,
        NULL_ARG,
        NULL_ARG,
        NULL_ARG);
  }

  private void initializeScheduler() {
    schedulerManager.createScheduler();
  }

  private void setTaskReactorWorkersNumber() {
    session
        .sql(
            String.format(
                "CALL %s.%s(%d, %s)",
                TASK_REACTOR_SCHEMA,
                SET_WORKERS_NUMBER_PROCEDURE,
                5,
                asVarchar(TASK_REACTOR_INSTANCE)))
        .collect();
  }
}
