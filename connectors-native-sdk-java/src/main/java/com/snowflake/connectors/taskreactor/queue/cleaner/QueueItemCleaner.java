/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.cleaner;

import com.snowflake.connectors.common.object.Identifier;
import com.snowflake.connectors.taskreactor.config.ConfigRepository;
import com.snowflake.connectors.taskreactor.queue.WorkItemQueue;
import com.snowflake.snowpark_java.Session;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class responsible for removal of obsolete items from the WorkItemQueue. */
public class QueueItemCleaner {

  private static final Logger logger = LoggerFactory.getLogger(QueueItemCleaner.class);
  private final ExpiredWorkSelector expiredWorkSelector;
  private final ConfigRepository configRepository;
  private final WorkItemQueue workItemQueue;

  /**
   * Returns a new instance of the {@link QueueItemCleaner QueueItemCleaner}
   *
   * @param session Snowpark session object
   * @param instanceSchema Task Reactor schema instance
   * @return a new cleaner instance
   */
  public static QueueItemCleaner from(Session session, Identifier instanceSchema) {
    return new QueueItemCleaner(
        ExpiredWorkSelector.getInstance(session),
        ConfigRepository.getInstance(session, instanceSchema),
        WorkItemQueue.getInstance(session, instanceSchema));
  }

  QueueItemCleaner(
      ExpiredWorkSelector expiredWorkSelector,
      ConfigRepository configRepository,
      WorkItemQueue workItemQueue) {
    this.expiredWorkSelector = expiredWorkSelector;
    this.configRepository = configRepository;
    this.workItemQueue = workItemQueue;
  }

  /** Removes obsolete items from WorkItemQueue. */
  public void clean() {
    logger.debug("Attempting to clean obsolete items in WorkItemsQueue.");
    String selectorName = configRepository.getConfig().expiredWorkSelector();
    List<String> ids = expiredWorkSelector.getExpired(selectorName);

    workItemQueue.delete(ids);
  }
}
