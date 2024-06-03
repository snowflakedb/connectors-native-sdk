/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_github_connector.api;

import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.APP_ROLE;
import static com.snowflake.connectors.sdk.example_github_connector.ingestion.Ingestion.DEST_SCHEMA;
import static java.lang.String.format;

import com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure;
import com.snowflake.connectors.sdk.example_github_connector.application.StateApis;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorManagementProcedures {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorManagementProcedures.class);

  public String provision_connector(Session session, Variant configuration) {
    logger.info("Provisioning connector");
    Map<String, Variant> map = configuration.asMap();
    String integrationName = map.get("external_access_integration_name").asString();
    String tokenName = map.get("secret_name").asString();
    String destinationDatabaseName = map.get("destination_database").asString();

    Map<String, String> connectionConfiguration =
        Map.of(
            "external_access_integration_name", integrationName,
            "secret_name", map.get("secret_name").asString(),
            "destination_database", map.get("destination_database").asString());
    setConfigValue(session, "config", new Variant(connectionConfiguration));
    alterConnectivityUdf(session, tokenName, integrationName);
    createDestinationDatabase(session, destinationDatabaseName);

    logger.info("Connector provisioned");
    return "Connector provisioned";
  }

  private void createDestinationDatabase(Session session, String databaseName) {
    session.sql(format("CREATE DATABASE %s", databaseName)).collect();
    session
        .sql(format("GRANT USAGE ON DATABASE %s TO APPLICATION ROLE %s", databaseName, APP_ROLE))
        .collect();
    session.sql(format("CREATE SCHEMA IF NOT EXISTS %s." + DEST_SCHEMA, databaseName)).collect();
    session
        .sql(
            format(
                "GRANT USAGE ON SCHEMA %s." + DEST_SCHEMA + " TO APPLICATION ROLE %s",
                databaseName,
                APP_ROLE))
        .collect();
  }

  private void alterConnectivityUdf(Session session, String secretName, String integrationName) {
    String connectivityUdfName = "PUBLIC.INGEST_DATA(STRING)";
    String query =
        format(
            "ALTER PROCEDURE %s \n"
                + " SET SECRETS=('%s' = %s)\n"
                + " EXTERNAL_ACCESS_INTEGRATIONS=(%s)",
            connectivityUdfName, Infrastructure.SECRET_KEY_NAME, secretName, integrationName);
    session.sql(query).collect();
  }

  public String setConfigValue(Session session, String configName, Variant configValue) {
    var table = StateApis.configApi(session);
    table.merge(configName, configValue);
    return "Set config " + configName + " to value " + configValue.asJsonString();
  }
}
