package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import static com.snowflake.connectors.sdk.example_github_connector.api.ResourcesManagementApi.createTableAndView;
import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.connectors.sdk.example_github_connector.application.StateApis;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Row;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ingestion {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  static Logger logger = LoggerFactory.getLogger(Ingestion.class);
  public static final String DEST_SCHEMA = "PUBLIC";

  public Variant ingest_data(Session session, String resourceId) {
    var state = StateApis.ingestionState(session);
    try {

      state.merge(resourceId, new Variant(Map.of("state", IngestionState.RUNNING.toString())));

      var split = resourceId.split("/");
      String organization = split[0];
      String repository = split[1];

      String url = format("https://api.github.com/repos/%s/%s/issues", organization, repository);
      String destTableName = createTableAndView(session, resourceId);

      while (true) {
        IngestionResult result = fetch_single_batch(session, url, destTableName);
        state.merge(
            resourceId,
            new Variant(
                Map.of(
                    "state", IngestionState.RUNNING.toString(), "ingestion", new Variant(result))));

        if (result.getExtras().getNextPageUrl() == null) {
          break;
        }
        url = result.getExtras().getNextPageUrl();
      }
      state.merge(resourceId, new Variant(Map.of("state", IngestionState.DONE.toString())));
      logger.info("Ingestion done for {}", resourceId);
      return new Variant("Stored data to table " + destTableName);
    } catch (Exception e) {
      state.merge(
          resourceId,
          new Variant(
              Map.of(
                  "state",
                  IngestionState.FAILED.toString(),
                  "reason",
                  format("Error message: %s", e.getMessage()))));
      return new Variant(
          String.format(
              "Ingestion of resource %s failed with error: %s", resourceId, e.getMessage()));
    }
  }

  public IngestionResult fetch_single_batch(Session session, String url, String destTableName) {
    HttpResponse<String> response = new GithubApiHttpClient().get(url);
    String body = response.body();
    Map[] issues;
    try {
      issues = objectMapper.readValue(body, Map[].class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot parse json", e);
    }

    StructType rawSchema = StructType.create(new StructField("raw", DataTypes.VariantType));
    List<Row> list =
        Arrays.stream(issues).map(v -> Row.create(new Variant(v))).collect(Collectors.toList());

    var source = session.createDataFrame(list.toArray(new Row[] {}), rawSchema);
    var target = session.table(destTableName);

    target
        .merge(source, target.col("raw").subField("id").equal_to(source.col("raw").subField("id")))
        .whenMatched()
        .updateColumn(Map.of("raw", source.col("raw")))
        .whenNotMatched()
        .insert(new Column[] {source.col("raw")})
        .collect();

    String nextUrl = parseLinks(response);
    return IngestionResult.createIngestionState(session, url, destTableName, nextUrl);
  }

  private String parseLinks(HttpResponse<String> response) {
    return response
        .headers()
        .firstValue("Link")
        .map((it) -> new LinkHeaderParser().parseLink(it))
        .map((it) -> it.get("next"))
        .orElse(null);
  }
}
