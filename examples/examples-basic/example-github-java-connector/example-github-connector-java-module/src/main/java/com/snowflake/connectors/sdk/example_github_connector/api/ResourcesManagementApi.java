/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_github_connector.api;

import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.APP_ROLE;
import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.TASKS_SCHEMA;
import static java.lang.String.format;

import com.snowflake.connectors.sdk.example_github_connector.application.StateApis;
import com.snowflake.connectors.sdk.example_github_connector.ingestion.Ingestion;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourcesManagementApi {
  private static final Logger logger = LoggerFactory.getLogger(ResourcesManagementApi.class);

  public String enable_resource(Session session, String resourceId) {
    logger.info("Enabling resource {}", resourceId);
    var table = StateApis.resourcesConfiguration(session);
    Map<String, Object> resourceConfiguration = Map.of("enabled", true);
    table.merge(resourceId, new Variant(resourceConfiguration));

    createTask(session, resourceId);
    createTableAndView(session, resourceId);

    logger.info("Resource {} enabled", resourceId);
    return format("Resource %s enabled.", resourceId);
  }

  private void createTask(Session session, String resourceId) {

    String taskName =
        format(
            "%s.INGEST_%s",
            TASKS_SCHEMA, resourceId.replaceAll("/", "_").replaceAll("-", "_").toUpperCase());
    String createTaskDdl =
        format(
            "CREATE OR REPLACE TASK IDENTIFIER('%s') WAREHOUSE = reference('warehouse_reference') SCHEDULE = '15 minutes' AS CALL"
                + " PUBLIC.INGEST_DATA('%s')",
            taskName, resourceId);
    session.sql(createTaskDdl).collect();

    session.sql(format("ALTER TASK IDENTIFIER('%s') RESUME", taskName)).collect();
    session.sql(format("EXECUTE TASK IDENTIFIER('%s')", taskName)).collect();
  }

  private static void createTableAndView(
      Session session, String destTableName, String destViewName) {
    session
        .sql(format("CREATE TABLE IF NOT EXISTS IDENTIFIER('%s') ( RAW VARIANT )", destTableName))
        .collect();
    session
        .sql(format("GRANT SELECT ON TABLE %s TO APPLICATION ROLE %s", destTableName, APP_ROLE))
        .collect();

    session
        .sql(
            format(
                "CREATE VIEW IF NOT EXISTS IDENTIFIER('%s') AS ( "
                    + " SELECT RAW:id as id,\n"
                    + " RAW:state as state,\n"
                    + " RAW:title as title,\n"
                    + " RAW:created_at as created_at,\n"
                    + " RAW:updated_at as updated_at,\n"
                    + " RAW:assignee:login as assignee\n"
                    + " from %s "
                    + " )",
                destViewName, destTableName))
        .collect();
    session
        .sql(format("GRANT SELECT ON VIEW %s TO APPLICATION ROLE %s", destViewName, APP_ROLE))
        .collect();
  }

  public static String createTableAndView(Session session, String resourceId) {
    var config = StateApis.configApi(session);
    Map<String, Variant> configuration = config.getValue("config").asMap();
    String destDatabaseName = configuration.get("destination_database").toString();
    var sqlFriendlyResourceName =
        resourceId.replaceAll("/", "_").replaceAll("-", "_").toUpperCase();

    String destTableName =
        destDatabaseName + "." + Ingestion.DEST_SCHEMA + "." + sqlFriendlyResourceName;
    String destViewName =
        destDatabaseName + "." + Ingestion.DEST_SCHEMA + "." + sqlFriendlyResourceName + "_VIEW";

    createTableAndView(session, destTableName, destViewName);
    return destTableName;
  }
}
