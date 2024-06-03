/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

import static com.snowflake.connectors.example.http.GithubHttpHelper.isSuccessful;

import com.snowflake.connectors.application.configuration.connection.ConnectionConfigurationHandler;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.connectors.example.http.GithubHttpHelper;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Backend implementation for the custom {@code PUBLIC.TEST_CONNECTION} procedure, used by the
 * {@link ConnectionConfigurationHandler} for initial external connection testing.
 *
 * <p>For this procedure to work - it must have been altered by the {@link
 * GithubConnectionConfigurationCallback} first.
 */
public class GithubConnectionValidator {

  private static final String ERROR_CODE = "TEST_CONNECTION_FAILED";

  public static Variant testConnection(Session session) {
    Configuration.fromConnectionConfig(session);
    return testOctocat().toVariant();
  }

  private static ConnectorResponse testOctocat() {
    try {
      var response = GithubHttpHelper.octocat();

      if (isSuccessful(response.statusCode())) {
        return ConnectorResponse.success();
      } else {
        return ConnectorResponse.error(
            ERROR_CODE,
            "Test connection to GitHub API failed. The response code is not included in the success"
                + " response code group.");
      }
    } catch (Exception exception) {
      return ConnectorResponse.error(
          ERROR_CODE, "Test connection to GitHub API failed " + exception.getMessage());
    }
  }
}
