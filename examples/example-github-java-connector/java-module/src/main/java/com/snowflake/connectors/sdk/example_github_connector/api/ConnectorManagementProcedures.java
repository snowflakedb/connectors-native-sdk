package com.snowflake.connectors.sdk.example_github_connector.api;

import com.snowflake.connectors.sdk.common.DefaultKeyValueTable;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

import java.util.Map;

import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.APP_ROLE;
import static java.lang.String.format;

public class ConnectorManagementProcedures {
    public String provision_connector(
            Session session,
            Variant configuration
    ) {
        Map<String, Variant> map = configuration.asMap();
        String integrationName = map.get("external_access_integration_name").asString();
        String tokenName = map.get("secret_name").asString();
        String destinationDatabaseName = map.get("destination_database").asString();

        Map<String, String> connectionConfiguration = Map.of(
                "external_access_integration_name", integrationName,
                "secret_name", map.get("secret_name").asString(),
                "destination_database", map.get("destination_database").asString(),
                "warehouse", map.get("warehouse").asString()
        );
        setConfigValue(session, "config", new Variant(connectionConfiguration));
        alterConnectivityUdf(session, tokenName, integrationName);
        createDestinationDatabase(session, destinationDatabaseName);

        return "Connector provisioned";
    }

    private void createDestinationDatabase(Session session, String databaseName) {
        session.sql(format("CREATE DATABASE %s", databaseName)).collect();
        session.sql(format("GRANT USAGE ON DATABASE %s TO APPLICATION ROLE %s", databaseName, APP_ROLE)).collect();
        session.sql(format("CREATE SCHEMA %s.DEST_SCHEMA", databaseName)).collect();
        session.sql(format("GRANT USAGE ON SCHEMA %s.DEST_SCHEMA TO APPLICATION ROLE %s", databaseName, APP_ROLE)).collect();
    }

    public String configure_connector(Session session, String secretName, String apiIntegrationName, String databaseName) {
        setConfigValue(session, "gh_secret_name", new Variant(secretName));
        alterConnectivityUdf(session, secretName, apiIntegrationName);

        createDestinationDatabase(session, databaseName);

        var table = new DefaultKeyValueTable(session, "STATE.APP_CONFIGURATION");
        table.update("dest_database_name", new Variant(databaseName));
        return "Connector configured.";
    }

    private void alterConnectivityUdf(Session session, String secretName, String integrationName) {
        String connectivityUdfName = "PUBLIC.INGEST_DATA(STRING)";
        String query = format("ALTER PROCEDURE %s \n" +
                " SET SECRETS=('token' = %s)\n" +
                " EXTERNAL_ACCESS_INTEGRATIONS=(%s)",
                connectivityUdfName,
                secretName,
                integrationName
        );
        session.sql(query).collect();
    }

    public String setConfigValue(Session session, String configName, Variant configValue) {
        var table = new DefaultKeyValueTable(session, "STATE.APP_CONFIGURATION");
        table.update(configName, configValue);
        return "Set config " + configName + " to value " + configValue.asJsonString();
    }
}
