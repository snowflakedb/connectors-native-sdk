/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.snowpark_java.types.Variant;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A utility class for GitHub HTTP calls. */
public final class GithubHttpHelper {

  private static final String OCTOCAT_URL = "https://api.github.com/octocat";
  private static final String ORG_REPO_URL = "https://api.github.com/repos/%s/%s";
  private static final String GET_REPO_ISSUES_URL = "https://api.github.com/repos/%s/%s/issues";
  private static final GithubApiHttpClient githubClient = new GithubApiHttpClient();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private GithubHttpHelper() {}

  public static HttpResponse<String> octocat() {
    return githubClient.get(OCTOCAT_URL);
  }

  public static HttpResponse<String> validateSource(String organization, String repository) {
    return githubClient.get(String.format(ORG_REPO_URL, organization, repository));
  }

  public static List<Variant> fetchRepoIssues(String organization, String repository) {
    return fetchAllBatches(String.format(GET_REPO_ISSUES_URL, organization, repository));
  }

  /**
   * Fetch all results from the initial URL and any subsequent results pages.
   *
   * @param initialUrl Initial URL
   * @return Results of the fetch
   */
  private static List<Variant> fetchAllBatches(String initialUrl) {
    var fetchResult = fetchSingleBatch(initialUrl);
    var combinedResults = new ArrayList<>(fetchResult.results);

    while (fetchResult.nextPageUrl != null) {
      fetchResult = fetchSingleBatch(fetchResult.nextPageUrl);
      combinedResults.addAll(fetchResult.results);
    }

    return combinedResults;
  }

  /**
   * Fetch all results from a single results page.
   *
   * @param url URL
   * @return Results of the fetch
   */
  private static GithubFetchResult fetchSingleBatch(String url) {
    var response = githubClient.get(url);
    var body = response.body();

    try {
      var batch =
          Arrays.stream(objectMapper.readValue(body, Map[].class))
              .map(Variant::new)
              .collect(Collectors.toList());
      return new GithubFetchResult(batch, url, parseLinks(response));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot parse json", e);
    }
  }

  /**
   * Parse response headers to find a possible next results page.
   *
   * @param response Response from the HTTP call
   * @return Next page URL or null if no found
   */
  private static String parseLinks(HttpResponse<String> response) {
    return response
        .headers()
        .firstValue("Link")
        .map(LinkHeaderParser::parseLink)
        .map((it) -> it.get("next"))
        .orElse(null);
  }

  public static boolean isSuccessful(int statusCode) {
    return statusCode >= 200 && statusCode <= 299;
  }
}
