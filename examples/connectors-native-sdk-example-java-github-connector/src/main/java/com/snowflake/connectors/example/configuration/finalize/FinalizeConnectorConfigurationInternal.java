/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_DATABASE;
import static com.snowflake.connectors.application.configuration.connector.ConnectorConfigurationKey.DESTINATION_SCHEMA;
import static com.snowflake.connectors.example.ConnectorObjects.INITIALIZE_INSTANCE_PROCEDURE;
import static com.snowflake.connectors.example.ConnectorObjects.ISSUES_TABLE;
import static com.snowflake.connectors.example.ConnectorObjects.ISSUES_VIEW;
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
 * FinalizeConnectorConfigurationCustomHandler}, providing final connector configuration logic:
 *
 * <ul>
 *   <li>destination schema and database setup
 *   <li>task reactor instance initialization
 *   <li>scheduler initialization
 * </ul>
 */
public class FinalizeConnectorConfigurationInternal implements FinalizeConnectorCallback {

  private static final Reference WAREHOUSE_REFERENCE = Reference.from("WAREHOUSE_REFERENCE");
  private static final String NULL_ARG = "null";

  private final Session session;
  private final SchedulerManager schedulerManager;

  public FinalizeConnectorConfigurationInternal(
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
        ObjectName.from(destinationDatabase.get(), destinationSchema.get(), ISSUES_TABLE)
            .getValue();
    var issuesView =
        ObjectName.from(destinationDatabase.get(), destinationSchema.get(), ISSUES_VIEW).getValue();

    createDestinationDbObjects(destinationDatabase.get(), destinationSchema.get());
    createIssuesTable(destinationTableName);
    createIssuesView(issuesView, destinationTableName);
    initializeTaskReactorInstance();
    initializeScheduler();
    setTaskReactorWorkersNumber();

    return ConnectorResponse.success();
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

  private void createIssuesTable(String destinationTable) {
    session
        .sql(
            format(
                "CREATE TABLE IF NOT EXISTS %s ("
                    + "ORGANIZATION VARCHAR,"
                    + " REPOSITORY VARCHAR,"
                    + " RAW_DATA VARIANT)",
                destinationTable))
        .collect();
    session
        .sql(format("GRANT ALL ON TABLE %s TO APPLICATION ROLE ADMIN", destinationTable))
        .collect();
    session
        .sql(format("GRANT SELECT ON TABLE %s TO APPLICATION ROLE DATA_READER", destinationTable))
        .collect();
  }

  private void createIssuesView(String issuesView, String destinationTable) {
    session
        .sql(
            format(
                "CREATE VIEW IF NOT EXISTS %s AS ( "
                    + " SELECT RAW_DATA:id as id,\n"
                    + " ORGANIZATION,\n"
                    + " REPOSITORY,\n"
                    + " RAW_DATA:state as state,\n"
                    + " RAW_DATA:title as title,\n"
                    + " RAW_DATA:created_at as created_at,\n"
                    + " RAW_DATA:updated_at as updated_at,\n"
                    + " RAW_DATA:assignee:login as assignee\n"
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
