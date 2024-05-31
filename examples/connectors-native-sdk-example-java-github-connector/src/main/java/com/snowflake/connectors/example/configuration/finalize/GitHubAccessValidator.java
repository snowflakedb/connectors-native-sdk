/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.finalize;

import static java.lang.String.format;

import com.snowflake.connectors.application.configuration.finalization.SourceValidator;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.example.configuration.utils.Configuration;
import com.snowflake.connectors.example.http.GithubHttpHelper;
import com.snowflake.snowpark_java.types.Variant;

/**
 * Custom implementation of {@link SourceValidator}, used by the {@link
 * FinalizeConnectorConfigurationCustomHandler}, providing final validation of GitHub API
 * connectivity and access token validity.
 */
public class GitHubAccessValidator implements SourceValidator {

  private static final String ORGANIZATION_KEY = "org_name";
  private static final String REPOSITORY_KEY = "repo_name";
  private static final String REPO_NOT_FOUND_ERROR = "REPO_NOT_FOUND";
  private static final String REPO_NOT_FOUND_MSG =
      "Repository does not exist or insufficient privileges granted to access token.";
  private static final String INVALID_ACCESS_TOKEN_ERROR = "INVALID_ACCESS_TOKEN";
  private static final String INVALID_ACCESS_TOKEN_MSG =
      "Github Access Token stored in connector configuration is invalid. Make sure the token has"
          + " not expired and repeat Connection Configuration step.";
  private static final String UNHANDLED_RESPONSE_CODE_ERROR = "UNHANDLED_RESPONSE_CODE";
  private static final String UNHANDLED_RESPONSE_CODE_MSG =
      "Response code [%s] received in response from GitHub API is unhandled by the connector.";

  @Override
  public ConnectorResponse validate(Variant variant) {
    var finalizeProperties = Configuration.fromCustomConfig(variant);
    var organization = finalizeProperties.getValue(ORGANIZATION_KEY);
    var repository = finalizeProperties.getValue(REPOSITORY_KEY);

    if (organization.isEmpty()) {
      return Configuration.keyNotFoundResponse(ORGANIZATION_KEY);
    }

    if (repository.isEmpty()) {
      return Configuration.keyNotFoundResponse(REPOSITORY_KEY);
    }

    var httpResponse = GithubHttpHelper.validateSource(organization.get(), repository.get());
    return prepareConnectorResponse(httpResponse.statusCode());
  }

  private ConnectorResponse prepareConnectorResponse(int statusCode) {
    switch (statusCode) {
      case 200:
        return ConnectorResponse.success();
      case 401:
        return ConnectorResponse.error(INVALID_ACCESS_TOKEN_ERROR, INVALID_ACCESS_TOKEN_MSG);
      case 404:
        return ConnectorResponse.error(REPO_NOT_FOUND_ERROR, REPO_NOT_FOUND_MSG);
      default:
        return ConnectorResponse.error(
            UNHANDLED_RESPONSE_CODE_ERROR, format(UNHANDLED_RESPONSE_CODE_MSG, statusCode));
    }
  }
}
