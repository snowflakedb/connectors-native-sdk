/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.cleaner;

import java.util.ArrayList;
import java.util.List;

/** In memory implementation of {@link ExpiredWorkSelector}. */
public class InMemoryExpiredWorkSelector implements ExpiredWorkSelector {

  private final List<String> store = new ArrayList<>();

  @Override
  public List<String> getExpired(String viewName) {
    return store;
  }

  /**
   * Adds new queue item ids for selection.
   *
   * @param ids queue item ids
   */
  public void addIdentifiers(List<String> ids) {
    store.addAll(ids);
  }

  /** Clears this selector. */
  public void clear() {
    store.clear();
  }
}
