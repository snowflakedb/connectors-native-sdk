/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.queue;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;
import java.sql.Timestamp;
import java.util.Map;

/** Representation of an item inserted into a task reactor queue. */
@JsonSerialize(using = QueueItemSerializer.class)
@JsonDeserialize(using = QueueItemDeserializer.class)
public class QueueItem {

  /** Item id. */
  public final String id;

  /** Item insertion timestamp. */
  public final Timestamp timestamp;

  /** Ingested resource id. */
  public final String resourceId;

  /** Should ongoing executions of the resource ingestion be cancelled. */
  public final boolean cancelOngoingExecution;

  /** Additional payload for the worker */
  public final Variant workerPayload;

  QueueItem(
      String id,
      Timestamp timestamp,
      String resourceId,
      boolean cancelOngoingExecution,
      Variant workerPayload) {
    this.id = id;
    this.timestamp = timestamp;
    this.resourceId = resourceId;
    this.cancelOngoingExecution = cancelOngoingExecution;
    this.workerPayload = workerPayload;
  }

  /**
   * Creates a new queue item from a specified map.
   *
   * @param map item map
   * @return new queue item instance
   */
  public static QueueItem fromMap(Map<String, Variant> map) {
    return new QueueItem(
        map.get("id").toString(),
        map.get("timestamp").asTimestamp(),
        map.get("resourceId").toString(),
        false,
        map.get("workerPayload"));
  }

  /**
   * Creates a new queue item from a specified row.
   *
   * @param row item row
   * @return new queue item instance
   */
  public static QueueItem fromRow(Row row) {
    boolean cancelOngoingExecution = false;
    if (!row.isNullAt(3)) {
      Map<String, Variant> dispatcherOptions = row.getVariant(3).asMap();
      cancelOngoingExecution =
          dispatcherOptions.getOrDefault("cancelOngoingExecution", new Variant(false)).asBoolean();
    }
    return new QueueItem(
        row.getString(0),
        row.getTimestamp(1),
        row.getString(2),
        cancelOngoingExecution,
        row.getVariant(4));
  }
}
