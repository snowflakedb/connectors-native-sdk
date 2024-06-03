/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import com.snowflake.snowpark_java.Session;

public class IngestionResult {

  private String destTableName;
  private Long rowsCount;
  private Extras extras;

  public IngestionResult() {}

  public IngestionResult(String destTableName, Long rowsCount, Extras extras) {
    this.destTableName = destTableName;
    this.rowsCount = rowsCount;
    this.extras = extras;
  }

  public String getDestTableName() {
    return destTableName;
  }

  public Long getRowsCount() {
    return rowsCount;
  }

  public Extras getExtras() {
    return extras;
  }

  public void setDestTableName(String destTableName) {
    this.destTableName = destTableName;
  }

  public void setRowsCount(Long rowsCount) {
    this.rowsCount = rowsCount;
  }

  public void setExtras(Extras extras) {
    this.extras = extras;
  }

  public static class Extras {
    private String url;
    private String nextPageUrl;

    public Extras() {}

    public Extras(String url, String nextPageUrl) {
      this.url = url;
      this.nextPageUrl = nextPageUrl;
    }

    public String getUrl() {
      return url;
    }

    public String getNextPageUrl() {
      return nextPageUrl;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public void setNextPageUrl(String nextPageUrl) {
      this.nextPageUrl = nextPageUrl;
    }
  }

  public static IngestionResult createIngestionState(
      Session session, String url, String destTableName, String nextUrl) {
    IngestionResult.Extras extras = new IngestionResult.Extras(url, nextUrl);
    return new IngestionResult(destTableName, session.table(destTableName).count(), extras);
  }
}
