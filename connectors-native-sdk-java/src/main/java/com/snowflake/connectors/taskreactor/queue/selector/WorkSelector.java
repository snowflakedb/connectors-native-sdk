/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Class representing selection mechanism from WorkItemQueue. */
public interface WorkSelector {

  /**
   * Selects all items returned by the view.
   *
   * @param workSelectorView Name of the snowflake view to be selected from.
   * @return Items from the view.
   */
  List<QueueItem> selectItemsFromView(String workSelectorView);

  /**
   * Calls selector procedure with provided queueItems.
   *
   * @param queueItems Items to be filtered by procedure.
   * @param workSelectorProcedure Snowflake procedure name.
   * @return Filtered items.
   */
  List<QueueItem> selectItemsUsingProcedure(
      List<QueueItem> queueItems, String workSelectorProcedure);

  /**
   * Gets a new instance of the default selector implementation.
   *
   * <p>Default implementation of the selector uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultWorkSelector DefaultWorkSelector}.
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new selector instance
   */
  static WorkSelector getInstance(Session session) {
    return new DefaultWorkSelector(session);
  }
}
