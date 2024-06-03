/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import com.snowflake.connectors.taskreactor.queue.QueueItem;
import java.util.ArrayList;
import java.util.List;

/** In memory implementation of {@link WorkSelector}. */
public class InMemoryWorkSelector implements WorkSelector {

  private final List<QueueItem> store = new ArrayList<>();

  @Override
  public List<QueueItem> selectItemsFromView(String workSelectorView) {
    return store;
  }

  @Override
  public List<QueueItem> selectItemsUsingProcedure(
      List<QueueItem> queueItems, String workSelectorProcedure) {
    return store;
  }

  /**
   * Adds new queue items for selection.
   *
   * @param items queue items
   */
  public void addItems(List<QueueItem> items) {
    store.addAll(items);
  }

  /** Clears this selector. */
  public void clear() {
    store.clear();
  }
}
