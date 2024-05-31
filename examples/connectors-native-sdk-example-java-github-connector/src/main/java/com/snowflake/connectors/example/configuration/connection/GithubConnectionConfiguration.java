/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.configuration.connection;

/** Simple constant aggregation class for GitHub connection configuration parameters. */
public final class GithubConnectionConfiguration {

  /** Configuration key for the GitHub external access integration name. */
  public static final String INTEGRATION_PARAM = "external_access_integration";

  /** Configuration key for the GitHub token secret name. */
  public static final String SECRET_PARAM = "secret";

  /** GitHub token name set in the procedures altered with the external access. */
  public static final String TOKEN_NAME = "github_api_token";

  private GithubConnectionConfiguration() {}
}
