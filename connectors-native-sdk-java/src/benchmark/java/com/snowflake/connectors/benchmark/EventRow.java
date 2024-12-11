/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.benchmark;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.Variant;

public class EventRow {
  private final String eventType;
  private final Variant payload;

  public static EventRow from(Row row) {
    return new EventRow(row.getString(0), row.getVariant(1));
  }

  public EventRow(String eventType, Variant payload) {
    this.eventType = eventType;
    this.payload = payload;
  }

  public String getEventType() {
    return eventType;
  }

  public Variant getPayload() {
    return payload;
  }

  public int getIntValue() {
    return payload.asMap().get("value").asInt();
  }

  public boolean isWorkerWorkingTimeEvent() {
    return "TASK_REACTOR_WORKER_WORKING_TIME".equals(eventType);
  }

  public boolean isWorkerIdleTimeEvent() {
    return "TASK_REACTOR_WORKER_IDLE_TIME".equals(eventType);
  }
}
