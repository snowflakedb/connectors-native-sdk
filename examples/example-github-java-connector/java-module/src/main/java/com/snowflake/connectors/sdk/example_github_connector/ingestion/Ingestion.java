package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.connectors.sdk.common.DefaultKeyValueTable;
import com.snowflake.connectors.sdk.example_github_connector.model.GithubIssue;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.SaveMode;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.Variant;

import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure.APP_ROLE;

public class Ingestion {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public String ingest_resource(Session session, String resourceName) {
        var table = new DefaultKeyValueTable(session, "STATE.APP_CONFIGURATION");
        String destDatabaseName = table.fetch("dest_database_name").asString();
        String destTableName = destDatabaseName + "." + "DEST_SCHEMA" + "." + resourceName;

        String url = String.format("https://api.github.com/repos/apache/%s/issues", resourceName);
        String query = String.format("SELECT rs.value AS result FROM LATERAL FLATTEN(INPUT => parse_json(PUBLIC.FETCH_DATA('%s'))) rs", url);
        session.sql(query).write().mode(SaveMode.Append).saveAsTable(destTableName);

        session.sql(String.format("GRANT SELECT ON TABLE %s TO APPLICATION ROLE %s", destTableName, APP_ROLE)).collect();
        return "Stored data to table " + destTableName;
    }

    public Variant ingest_data(Session session, String resourceId) {
        var table = new DefaultKeyValueTable(session, "STATE.APP_CONFIGURATION");
        Map<String, Variant> configuration = table.fetch("config").asMap();
        String destDatabaseName = configuration.get("destination_database").toString();

        String destTableName = destDatabaseName + "." + "DEST_SCHEMA" + "." + resourceId.replaceAll("/","_");

        var split = resourceId.split("/");
        String organization = split[0];
        String repository = split[1];

        String url = String.format("https://api.github.com/repos/%s/%s/issues", organization, repository);

        while (true) {
            FetchResult result = fetch_single_batch(session, url, destTableName);
            session.sql(String.format("GRANT SELECT ON TABLE %s TO APPLICATION ROLE %s", destTableName, APP_ROLE)).collect();
            if (result.getNextPageUrl() == null) {
                break;
            }
            url = result.getNextPageUrl();
        }

        return new Variant("Stored data to table " + destTableName);
    }

    class FetchResult {
        private final String nextPageUrl;

        FetchResult(String nextPageUrl) {
            this.nextPageUrl = nextPageUrl;
        }

        public String getNextPageUrl() {
            return nextPageUrl;
        }
    }

    public FetchResult fetch_single_batch(Session session, String url, String destTableName) {

        HttpResponse<String> response = new GithubApiHttpClient().get(url);

        String body = response.body();
        GithubIssue[] issues;
        try {
            issues = objectMapper.readValue(body, GithubIssue[].class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot parse json", e);
        }

        StructType schema =
            StructType.create(
                new StructField("id", DataTypes.IntegerType),
                new StructField("number", DataTypes.IntegerType),
                new StructField("title", DataTypes.StringType),
                new StructField("state", DataTypes.StringType),
                new StructField("node_id", DataTypes.StringType),
                new StructField("url", DataTypes.StringType)
            );

        List<Row> list = Arrays.stream(issues).map((i) -> Row.create(i.getId(), i.getNumber(), i.getTitle(), i.getState(), i.getNode_id(), i.getUrl())).collect(Collectors.toList());
        session.createDataFrame( list.toArray(new Row[]{}), schema).write().mode(SaveMode.Append).saveAsTable(destTableName);;
        return parseLinks(response);
    }

    private FetchResult parseLinks(HttpResponse<String> response) {
        return response.headers()
                .firstValue("Link")
                .map((it) -> new LinkHeaderParser().parseLink(it))
                .map((it) -> new FetchResult(it.get("next")))
                .orElse(new FetchResult(null));
    }
}
