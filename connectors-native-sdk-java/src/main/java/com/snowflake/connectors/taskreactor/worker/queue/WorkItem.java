/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.taskreactor.worker.queue;

import com.snowflake.connectors.taskreactor.queue.QueueItem;
import com.snowflake.snowpark_java.types.Variant;
import java.util.Objects;

/** Class describing an item that was inserted to the input queue of the task reactor. */
public final class WorkItem {

  /** Surrogate key for the row in the input queue. Each row gets its unique UUID id generated. */
  public final String id;

  /** Value of the {@code RESOURCE_ID} column inserted to the input queue. */
  public final String resourceId;

  /** Value of the {@code WORKER_PAYLOAD} column inserted to the input queue. */
  public final Variant payload;

  /**
   * Creates a new {@link WorkItem}.
   *
   * @param id work item id
   * @param resourceId resource id
   * @param payload item payload
   */
  public WorkItem(String id, String resourceId, Variant payload) {
    this.id = id;
    this.resourceId = resourceId;
    this.payload = payload;
  }

  /**
   * Creates a new {@link WorkItem} from a given {@link QueueItem}.
   *
   * @param queueItem queue item
   * @return a new work item
   */
  public static WorkItem from(QueueItem queueItem) {
    return new WorkItem(queueItem.id, queueItem.resourceId, queueItem.workerPayload);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkItem workItem = (WorkItem) o;
    return Objects.equals(id, workItem.id)
        && Objects.equals(resourceId, workItem.resourceId)
        && Objects.equals(payload, workItem.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, resourceId, payload);
  }

  @Override
  public String toString() {
    return String.format(
        "WorkItem[id='%s', resourceId='%s', payload='%s']",
        id, resourceId, payload == null ? null : payload.asJsonString());
  }
}
