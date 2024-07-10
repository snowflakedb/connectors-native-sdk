/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.http;

import static java.lang.String.format;

import com.snowflake.connectors.example.configuration.connection.GithubConnectionConfiguration;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** A simple HTTP client for GitHub API calls. Uses a secret (GitHub token) provided by the user. */
public class GithubApiHttpClient {

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

  private final HttpClient client;
  private final String secret;

  public GithubApiHttpClient() {
    this.client = HttpClient.newHttpClient();
    this.secret =
        SnowflakeSecrets.newInstance()
            .getOAuthAccessToken(GithubConnectionConfiguration.TOKEN_NAME);
  }

  /**
   * Perform a GET HTTP request.
   *
   * @param url Target URL
   * @return HTTP response
   */
  public HttpResponse<String> get(String url) {
    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .header("Authorization", format("Bearer %s", secret))
            .header("Content-Type", "application/json")
            .timeout(REQUEST_TIMEOUT)
            .build();

    try {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException(format("HttpRequest failed: %s", ex.getMessage()), ex);
    }
  }
}
