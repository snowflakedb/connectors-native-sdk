/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.cleaner;

import com.snowflake.snowpark_java.Session;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Default implementation onf {@link ExpiredWorkSelector}. */
public class DefaultExpiredWorkSelector implements ExpiredWorkSelector {

  private final Session session;

  DefaultExpiredWorkSelector(Session session) {
    this.session = session;
  }

  @Override
  public List<String> getExpired(String viewName) {
    return Arrays.stream(session.sql(String.format("SELECT ID FROM %s", viewName)).collect())
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
  }
}
