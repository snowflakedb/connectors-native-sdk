package com.snowflake.connectors.sdk.example_github_connector.api;

import com.snowflake.connectors.sdk.common.DefaultKeyValueTable;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

import java.util.Map;

import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.TASKS_SCHEMA;
import static java.lang.String.format;

public class ResourcesManagementApi {

    public String enable_resource(Session session, String resourceId) {
        var table = new DefaultKeyValueTable(session, "STATE.RESOURCE_CONFIGURATION");
        Map<String, Object> resourceConfiguration = Map.of("enabled", true);
        table.update(resourceId, new Variant(resourceConfiguration));

        createTask(session, resourceId);

        return format("Resource %s enabled.", resourceId);
    }

    private void createTask(Session session, String resourceId) {
        var table = new DefaultKeyValueTable(session, "STATE.APP_CONFIGURATION");
        Map<String, Variant> configuration = table.fetch("config").asMap();

        String warehouse = configuration.get("warehouse").toString();

        String taskName = format("%s.INGEST2_%s", TASKS_SCHEMA, resourceId.replaceAll("/", "_").toUpperCase());
        String createTaskDdl = format("CREATE OR REPLACE TASK IDENTIFIER('%s') WAREHOUSE = %s SCHEDULE = '15 minutes' AS CALL PUBLIC.INGEST_DATA('%s')", taskName, warehouse, resourceId);
        session.sql(createTaskDdl).collect();

        session.sql(format("EXECUTE TASK IDENTIFIER('%s')", taskName)).collect();
        session.sql(format("ALTER TASK IDENTIFIER('%s') RESUME", taskName)).collect();
    }

}
