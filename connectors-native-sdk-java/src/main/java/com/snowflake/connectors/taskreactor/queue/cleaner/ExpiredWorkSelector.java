/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.cleaner;

import com.snowflake.snowpark_java.Session;
import java.util.List;

/** Representation of the Task Reactor expired work selector. */
public interface ExpiredWorkSelector {

  /**
   * Fetches all unreachable items from the WorkItemQueue.
   *
   * @param viewName Name of the view used for selecting items.
   * @return List of identifiers of unreachable items.
   */
  List<String> getExpired(String viewName);

  /**
   * Returns a new instance of the default selector implementation.
   *
   * <p>Default implementation of the selector uses:
   *
   * <ul>
   *   <li>a default implementation of {@link DefaultExpiredWorkSelector
   *       DefaultExpiredWorkSelector}, created for the user defined view.
   * </ul>
   *
   * @param session Snowpark session object
   * @return a new view instance
   */
  static ExpiredWorkSelector getInstance(Session session) {
    return new DefaultExpiredWorkSelector(session);
  }
}
