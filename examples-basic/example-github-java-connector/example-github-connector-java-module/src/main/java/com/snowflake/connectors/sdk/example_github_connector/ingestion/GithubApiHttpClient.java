package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import com.snowflake.connectors.sdk.example_github_connector.application.Infrastructure;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class GithubApiHttpClient {
  private final HttpClient client;
  private final String secret;

  public GithubApiHttpClient() {
    this.client = HttpClient.newHttpClient();
    this.secret =
        SnowflakeSecrets.newInstance().getGenericSecretString(Infrastructure.SECRET_KEY_NAME);
  }

  public HttpResponse<String> get(String url) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .header("Authorization", String.format("Bearer %s", secret))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .build();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new RuntimeException(
            "HttpRequest request failed with response code: "
                + response.statusCode()
                + " and body: "
                + response.statusCode());
      }
      return response;
    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException("HttpRequest failed " + ex.getMessage(), ex);
    }
  }
}
