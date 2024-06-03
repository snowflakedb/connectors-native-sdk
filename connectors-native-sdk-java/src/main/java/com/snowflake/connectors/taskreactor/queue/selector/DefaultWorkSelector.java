/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue.selector;

import static java.util.stream.Collectors.toList;

import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation of {@link WorkSelector}. */
class DefaultWorkSelector implements WorkSelector {

  private static final Logger logger = LoggerFactory.getLogger(DefaultWorkSelector.class);

  private final Session session;

  DefaultWorkSelector(Session session) {
    this.session = session;
  }

  @Override
  public List<QueueItem> selectItemsFromView(String workSelectorView) {
    logger.debug("Selecting work items using view {}.", workSelectorView);

    Row[] rawRows =
        session
            .sql(
                String.format(
                    "SELECT ID, TIMESTAMP, RESOURCE_ID, DISPATCHER_OPTIONS, WORKER_PAYLOAD FROM %s",
                    workSelectorView))
            .select("ID", "TIMESTAMP", "RESOURCE_ID", "DISPATCHER_OPTIONS", "WORKER_PAYLOAD")
            .collect();
    return Arrays.stream(rawRows).map(QueueItem::fromRow).collect(toList());
  }

  @Override
  public List<QueueItem> selectItemsUsingProcedure(
      List<QueueItem> queueItems, String workSelectorProcedure) {
    logger.debug("Selecting work items using procedure {}.", workSelectorProcedure);

    String queueItemsSerialized = new Variant(queueItems).asJsonString();
    return session
        .sql(
            String.format("CALL %s(PARSE_JSON('%s'))", workSelectorProcedure, queueItemsSerialized))
        .first()
        .map(DefaultWorkSelector::rowToQueueItems)
        .orElse(Collections.emptyList());
  }

  private static List<QueueItem> rowToQueueItems(Row row) {
    return row.getListOfVariant(0).stream()
        .map(Variant::asMap)
        .map(QueueItem::fromMap)
        .collect(toList());
  }
}
