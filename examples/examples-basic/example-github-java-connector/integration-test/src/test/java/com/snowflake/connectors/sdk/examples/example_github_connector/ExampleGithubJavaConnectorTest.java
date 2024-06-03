/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.examples.example_github_connector;

import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ExampleGithubJavaConnectorTest extends BaseConnectorTest {

  @Test
  void shouldIngestData() {
    // given
    String configStr =
        format(
            "{\"warehouse\": \"XS\", \"destination_database\": \"%s\", \"secret_name\":"
                + " \"%s.PUBLIC.GITHUB_TOKEN\", \"external_access_integration_name\": \"%s\"}",
            destinationDatabaseName, secretsDatabaseName, API_INTEGRATION);
    session.sql(format("CALL PUBLIC.PROVISION_CONNECTOR(parse_json('%s'))", configStr)).collect();
    session.sql(format("USE DATABASE %s", applicationInstanceName)).collect();
    var repository = "snowflakedb/snowpark-python";

    // when
    var result = session.sql(format("CALL PUBLIC.INGEST_DATA('%s')", repository)).collect();

    // then
    assertEquals(
        format(
            "\"Stored data to table %s.PUBLIC.SNOWFLAKEDB_SNOWPARK_PYTHON\"",
            destinationDatabaseName),
        result[0].get(0).toString());
    assertEquals(1, result.length);

    var data =
        session
            .sql(
                format(
                    "SELECT * FROM %s.PUBLIC.SNOWFLAKEDB_SNOWPARK_PYTHON", destinationDatabaseName))
            .collect();
    assertTrue(data.length > 30);

    // and data can be seen in a view
    data =
        session
            .sql(
                format(
                    "SELECT * FROM %s.PUBLIC.SNOWFLAKEDB_SNOWPARK_PYTHON_VIEW",
                    destinationDatabaseName))
            .collect();
    assertTrue(data.length > 30);
  }

  @Test
  void shouldEnableResource() {
    // given
    String configStr =
        format(
            " {\"warehouse\": \"XS\", \"destination_database\": \"%s\", \"secret_name\":"
                + " \"%s.PUBLIC.GITHUB_TOKEN\", \"external_access_integration_name\": \"%s\"} ",
            destinationDatabaseName, secretsDatabaseName, API_INTEGRATION);
    session.sql(format("CALL PUBLIC.PROVISION_CONNECTOR(parse_json('%s'))", configStr)).collect();
    session.sql(format("USE DATABASE %s", applicationInstanceName)).collect();

    var repository = "snowflakedb/snowpark-python";

    // when
    var result = session.sql(format("CALL PUBLIC.ENABLE_RESOURCE('%s')", repository)).collect();

    // then function completes successfully
    assertEquals("Resource snowflakedb/snowpark-python enabled.", result[0].get(0));
    assertEquals(1, result.length);

    // and data lands in dest schema eventually
    await()
        .atMost(ofMinutes(1))
        .with()
        .pollInterval(ofSeconds(5))
        .until(
            () -> {
              try {
                var data =
                    session
                        .sql(
                            format(
                                "SELECT * from %s.PUBLIC.snowflakedb_snowpark_python",
                                destinationDatabaseName))
                        .collect();
                return data.length > 30;
              } catch (Exception e) {
                return false;
              }
            });

    // and data can be seen in a view
    var data =
        session
            .sql(
                format(
                    "SELECT * FROM %s.PUBLIC.SNOWFLAKEDB_SNOWPARK_PYTHON_VIEW",
                    destinationDatabaseName))
            .collect();
    assertTrue(data.length > 30);
  }
}
