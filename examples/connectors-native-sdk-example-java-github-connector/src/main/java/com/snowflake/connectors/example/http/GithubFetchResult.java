/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.example.http;

import com.snowflake.snowpark_java.types.Variant;
import java.util.List;

/**
 * A simple value object for the GitHub HTTP call results, URL used for the call and an optional URL
 * for the next results page.
 */
public class GithubFetchResult {

  public final List<Variant> results;
  public final String url;
  public final String nextPageUrl;

  public GithubFetchResult(List<Variant> results, String url, String nextPageUrl) {
    this.results = results;
    this.url = url;
    this.nextPageUrl = nextPageUrl;
  }
}
